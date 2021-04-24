# frozen_string_literal: true

require 'logging'
require 'fpgrowth'


module NoSE
  # Produces potential indices to be used in schemas
  class PrunedIndexEnumeratorSimplified < IndexEnumerator
    def initialize(workload, cost_model, is_entity_fields_shared_threshold,
                   index_plan_step_threshold, index_plan_shared_threshold, choice_limit_size: 3_000)
      @eq_threshold = 1
      @cost_model = cost_model
      @is_entity_fields_shared_threshold = is_entity_fields_shared_threshold
      @index_steps_threshold = index_plan_step_threshold
      @index_plan_shared_threshold = index_plan_shared_threshold
      @choice_limit_size = choice_limit_size
      super(workload)
    end

    def indexes_for_queries(queries, additional_indexes)
      return [] if queries.empty?

      indexes = Parallel.flat_map(queries, in_processes: [Parallel.processor_count - 5, 0].max()) do |query|
        idxs = indexes_for_query(query).to_a
        puts query.comment + ": " + idxs.size.to_s if query.instance_of? Query
        idxs
      end.uniq
      indexes += additional_indexes

      puts "index size before pruning: " + indexes.size.to_s
      indexes = get_used_indexes(queries, indexes)
      puts "index size after pruning: " + indexes.size.to_s

      indexes.uniq
    end

    # remove CFs that are not used in query plans
    def get_used_indexes(queries, indexes)
      puts "whole indexes : " + indexes.size.to_s
      get_trees(queries, indexes).flat_map do |tree|
        tree.flat_map(&:indexes).uniq
      end.uniq
    end

    def get_trees(queries, indexes)
      planner = Plans::PrunedQueryPlanner.new @workload.model, indexes, @cost_model, @index_steps_threshold
      #planner = Plans::QueryPlanner.new @workload.model, indexes, @cost_model
      #Parallel.map(queries, in_processes: [Parallel.processor_count - 5, 0].max()) do |query|
      # 一旦並列を無効化して、stack トレースでより詳しく詰まっている箇所を確認する
      queries.map do |query|
        planner.find_plans_for_query(query)
      end
    end

    # Produce all possible indices for a given query
    # @return [Array<Index>]
    def indexes_for_query(query)
      @logger.debug "Enumerating indexes for query #{query.text}"

      range = get_query_range query
      eq = get_query_eq query
      orderby = get_query_orderby query
      groupby = get_query_groupby query

      # query.subgrapphs() の中で query.graph に対しては materialized view を返す
      # query.subgraphs() で分割されたクエリグラフには接点となるエンティティがある。suffix query 側では必ずこのエンティティが partition key の先頭に来る
      # さらに、prefix query 側でのこの分割点に当たるエンティティが partition key の先頭に来ることはない。
      # unique なエンティティに対しては、グループ化するというのはこの enumerator でも取り入れても良さそう
      #
      # subgraph はエッジ分割で列挙するので、分割した結果のグラフには重複するエンティティはない
      # しかし、すべてのエンティティに対して列挙するので、重複するペアができる
      # ここで、entity サイズが1つのサブグラフもできるが、secondary index のように動作する CF とその対象のペアがあるので、全てのエンティティを持つサブグラフに対しても CF を列挙する必要がある
      # ただし、partition key の先頭の entity の属性順序以外は mv と同じ順番で問題ない。
      # はじめに graph.subgraphs で全ての subgraph を列挙して、ペアを作る
      # そして、[1 つ + 残りの全ての entity] のケースでは、全てのエンティティの subgraph では先頭の属性だけを順列列挙して残りの entity の分は MV と同じにする
      #
      # prefix graph は先頭の entity の属性の並びはクエリへの応答可能性に影響があるが、suffix graph では関係ない。
      # ここで、列挙数を減らせるはず
      #
      # indexes_for_choices で eq_coices.partitions をやっているが、suffix クエリにおいては、ある程度決め打ちできるはず

      indexes = group_subgraph(query.eq_fields, query.graph).flat_map do |subgraph_pair|
        overlapping_entities = subgraph_pair[:prefix].entities & subgraph_pair[:suffix].entities

        prefix_idxes = indexes_for_graph(subgraph_pair[:prefix], query.select, eq, range, orderby, overlapping_entities, is_prefix_graph: true)
        puts "====== prefix: #{subgraph_pair[:prefix].entities.map(&:name).inspect} ======="
        puts "prefix: #{prefix_idxes.size} ====="

        # 一つの entity のみの prefix + 全ての entity を持つ　suffix に対応するための列挙メソッドが必要
        if subgraph_pair[:prefix].entities.size == 1 and subgraph_pair[:suffix].entities == query.graph.entities
          STDERR.puts query.text
          suffix_idxes = indexes_for_full_suffix_graph(subgraph_pair[:prefix], subgraph_pair[:suffix], query.materialize_view, eq, range, orderby, groupby)
          puts "====== full suffix: #{subgraph_pair[:suffix].entities.map(&:name).inspect} ======="
          puts "suffix: #{suffix_idxes.size} ====="
        else
          suffix_idxes = indexes_for_graph(subgraph_pair[:suffix], query.select, eq, range, orderby, overlapping_entities, is_prefix_graph: false)
          puts "====== suffix: #{subgraph_pair[:suffix].entities.map(&:name).inspect} ======="
          puts "suffix: #{suffix_idxes.size} ====="
        end

        [prefix_idxes + suffix_idxes].flatten
      end
      indexes.uniq!
      puts "prune indexes based on clutering key before: #{indexes.size}"
      indexes = ignore_cluster_key_order query, indexes
      puts "prune indexes based on clutering key after: #{indexes.size}"
      indexes << query.materialize_view
      indexes << query.materialize_view_with_aggregation
      puts "#{indexes.size} indexes for #{query.comment}"
      indexes
    end

    # こいつの実装汚いのでもう少しなんとかしたい
    # prefix と suffix で実装を分けるだけでももう少し削れる余地がありそう
    def ignore_cluster_key_order(query, indexes)
      pruned_indexes = indexes.sort_by{|i| i.hash_str}.dup
      indexes.each_with_index do |target_index, idx|
        next unless target_index.key_fields.size > (query.eq_fields + query.order).size

        similar_indexes = indexes[(idx + 1)..-1].select{|i| target_index.hash_fields == i.hash_fields and \
                           target_index.order_fields.to_set == i.order_fields.to_set and \
                           target_index.extra == i.extra}
        similar_indexes.each do |other_index|
          variable_fields = (query.eq_fields + query.order + query.groupby + [query.range_field]).to_set
          worth_order_fields_candidates = (variable_fields & target_index.key_fields).to_set

          # ここを サイズベースではなく、属性一致によるものに変えられたら、より削れる余地があるはず
          variable_order_fields_size = [worth_order_fields_candidates.size - target_index.hash_fields.size, 0].max
          if target_index.order_fields.take(variable_order_fields_size) == other_index.order_fields.take(variable_order_fields_size)
            pruned_indexes = pruned_indexes.reject{|pi| pi == other_index}
          end
        end
      end
      pruned_indexes
    end


    def indexes_for_full_suffix_graph(prefix_graph, suffix_graph, materialize_view, eq, range, orderby, groupby)
      prefix_eq_choices = eq_choices prefix_graph, eq
      prefix_eq_choices.map do |eq_choice|
        generate_index(eq_choice, materialize_view.order_fields.reject{|of| eq_choice.include? of},
                       materialize_view.extra.reject{|e| eq_choice.include? e}, suffix_graph)
      end.compact
    end

    def group_subgraph(eq, parent_graph)
      subgraphs = parent_graph.subgraphs(recursive = false).to_a
      subgraph_pairs = []
      subgraphs.each_with_index do |subgraph, idx|
        subgraphs[(idx + 1)..-1].each do |other_subgraph|
          next unless (subgraph.entities & other_subgraph.entities).size == 1
          next unless (subgraph.entities | other_subgraph.entities) == parent_graph.entities
          next if subgraph.entities.size == 1 and \
                  not (other_subgraph.join_order(eq).take(1) == subgraph.join_order(eq) or \
                  other_subgraph.join_order(eq).reverse.take(1) == subgraph.join_order(eq))
          next if other_subgraph.entities.size == 1 and \
                  not (subgraph.join_order(eq).take(1) == other_subgraph.join_order(eq) or \
                  subgraph.join_order(eq).reverse.take(1) == other_subgraph.join_order(eq))
          next if (subgraph.entities.size == 1 and not eq.map(&:parent).include? subgraph.entities.first) or \
                  (other_subgraph.entities.size == 1 and not eq.map(&:parent).include? other_subgraph.entities.first)

          prefix_subgraph, suffix_subgraph = subgraph, other_subgraph
          if suffix_subgraph.entities.size == 1 and prefix_subgraph.entities.size > 1
            prefix_subgraph, suffix_subgraph = suffix_subgraph, prefix_subgraph
          end
          # choose prefix subgraph. this works only when parent_graph == query.graph
          unless eq.map(&:parent).include? prefix_subgraph.join_order(eq).first
            prefix_subgraph, suffix_subgraph = suffix_subgraph, prefix_subgraph
          end

          subgraph_pairs << {prefix: prefix_subgraph, suffix: suffix_subgraph}
        end
      end
      subgraph_pairs
    end

    # さらに、prefix query 側でのこの分割点に当たるエンティティが partition key の先頭に来ることはない。
    def indexes_for_graph(graph, select, eq, range, orderby, overlapping_entities, is_prefix_graph: true)
      eq_choices, order_choices, extra_choices = get_graph_choices graph, select, eq, range, orderby, overlapping_entities, is_prefix_graph: is_prefix_graph
      choices = eq_choices.product(extra_choices)

      choices = limit_choices choices
      indexes_for_choices(graph, choices, order_choices).uniq
    end

    def support_indexes(indexes, by_id_graph)
      STDERR.puts "start enumerating support indexes"
      indexes = indexes.map(&:to_id_graph).uniq if by_id_graph

      queries = support_queries indexes
      puts "support queries: " + queries.size.to_s
      support_indexes = indexes_for_queries queries, []
      STDERR.puts "end enumerating support indexes"
      support_indexes
    end

    private

    def limit_choices(choices)
      return choices if choices.size < @choice_limit_size

      base_size = choices.size
      # sort choices to always get the same reduced-choices
      choices = choices.sort_by do |choice|
        (choice.first.map(&:id) + choice.last.map(&:id)).join(',')
      end.take(@choice_limit_size)
      STDERR.puts "pruning choices from #{base_size} to #{choices.size}"
      choices
    end

    def get_graph_choices(graph, select, eq, range, orderby, overlapping_entities, is_prefix_graph: true)
      eq_choices = eq_choices graph, eq
      eq_choices = is_prefix_graph ?
                     prune_eq_choices_for_prefix_graph(eq_choices, eq, range, orderby)
                     : prune_eq_choices_for_suffix_graph(eq_choices, overlapping_entities)

      range_fields = graph.entities.map { |entity| range[entity] }.reduce(&:+).uniq
      order_choices = range_fields.prefixes.flat_map do |fields|
        fields.permutation.to_a
      end.uniq << []
      extra_choices = extra_choices(graph, select, eq, range)

      [eq_choices, order_choices, extra_choices]
    end

    def prune_eq_choices_for_prefix_graph(eq_choices, eq, range, orderby)
      eq_entities = eq.keys
      eq_choices = eq_choices.select do |eq_choice|
        eq_choice_entites = eq_choice.map(&:parent).uniq
        key_prefix_entities = (eq_entities & eq_choice_entites).uniq
        next false if key_prefix_entities.empty?

        # overlapping_eq_entities は eq.keys のどれか一つと一致していれば良い
        overlapping_eq_entities = eq_choice_entites.take(key_prefix_entities.size).to_set
        eq_entities.combination(overlapping_eq_entities.size)
                                .any?{|ek| ek.to_set == overlapping_eq_entities}
      end

      # eq_choices の各属性は、eq, range, orderby, groupby に含まれない限り、id_field が先頭にある必要がない。
      eq_choices = eq_choices.select do |eq_choice|
        # この CF は集約処理用の MV ではないので、以下のコードでは groupby は考慮しない
        non_query_specified_id_fields = eq_choice.select(&:primary_key)
                                                 .reject{|f| eq.values.flatten.include?(f) or
                                                   range.values.flatten.include?(f) or
                                                   orderby.values.flatten.include?(f)}
        next true if eq_choice.size <= 1
        query_specified_fields = eq_choice.to_set - non_query_specified_id_fields.to_set
        next true if non_query_specified_id_fields.empty? or query_specified_fields.empty?
        query_specified_fields.map{|qsf| eq_choice.index(qsf)}.max < non_query_specified_id_fields.map{|nqsif| eq_choice.index(nqsif)}.min
      end

      eq_choices
    end

    def prune_eq_choices_for_suffix_graph(eq_choices, overlapping_entities)
      fail if overlapping_entities.size > 1

      # suffix graph cf should start with overlapping entity
      eq_choices = eq_choices.select do |eq_choice|
        eq_choice.map(&:parent).uniq.first == overlapping_entities.first
      end

      ## suffix eq_choices are enough with starting by primary keys
      eq_choices = eq_choices.select do |eq_choice|
        eq_choice.first.primary_key?
      end
      eq_choices
    end

    def get_query_orderby(query)
      order = query.order.group_by(&:parent)
      order.default_proc = ->(*) { [] }
      order
    end

    def get_query_groupby(query)
      groupby = query.order.group_by(&:parent)
      groupby.default_proc = ->(*) { [] }
      groupby
    end
  end
end

