# frozen_string_literal: true

require 'logging'
require 'fpgrowth'


module NoSE
  # Produces potential indices to be used in schemas
  class PrunedIndexEnumerator < IndexEnumerator
    def initialize(workload, cost_model)
      @eq_threshold = 1
      @cost_model = cost_model
      super(workload)
    end

    def indexes_for_queries(queries, additional_indexes)
      return [] if queries.empty?
      entity_fields = get_frequent_entity_choices queries
      puts "entity_fields (pattern) size: " + entity_fields.size.to_s
      extra_fields = get_frequent_extra_choices queries

      #indexes = Parallel.flat_map(queries, in_processes: 12) do |query|
      indexes = queries.flat_map do |query|
        idxs = indexes_for_query(query, entity_fields[query], extra_fields).to_a
        used_idxes = get_used_indexes_in_query_plans [query], idxs
        idxs
      end
      indexes += additional_indexes

      get_used_indexes_in_query_plans queries, indexes
    end

    # remove CFs that are not used in query plans
    def get_used_indexes_in_query_plans(queries, indexes)
      puts "whole indexes : " + indexes.size.to_s
      planner = Plans::QueryPlanner.new @workload.model, indexes, @cost_model
      used_indexes = Parallel.flat_map(queries, in_processes: 4) do |query|
        plan = planner.find_plans_for_query(query)
        plan.flat_map{|p| p.steps.select{|s| s.is_a? Plans::IndexLookupPlanStep}.map(&:index)}.uniq
      end
      puts "used indexes : " + used_indexes.size.to_s
      used_indexes
    end

    # Produce all possible indices for a given query
    # @return [Array<Index>]
    def indexes_for_query(query, entity_fields, extra_fields)
      @logger.debug "Enumerating indexes for query #{query.text}"

      range = get_query_range query
      eq = get_query_eq query

      #indexes = Parallel.flat_map(query.graph.subgraphs, in_processes: 7) do |graph|
      indexes = query.graph.subgraphs.flat_map do |graph|
        indexes_for_graph graph, query.select, query.counts, query.sums, query.maxes, query.avgs, eq, range, query.groupby, entity_fields, extra_fields
      end.uniq << query.materialize_view

      puts "pie == #{query.text} =============================== " + indexes.size.to_s
      indexes
    end

    # Since support queries are simple, use IndexEnumerator to make migration easier
    def support_indexes(indexes, by_id_graph)
      index_enumerator = IndexEnumerator.new(@workload)
      return index_enumerator.support_indexes indexes, by_id_graph
    end

    private

    # Get all possible indices which jump a given piece of a query graph
    # @return [Array<Index>]
    def indexes_for_graph(graph, select, count, sum, max, avg, eq, range, group_by, entity_fields_patterns, extra_fields)
      eq_choices, order_choices, extra_choices = get_choices graph, select, eq, range, group_by, entity_fields_patterns, extra_fields

      ## Generate all possible indices based on the field choices
      choices = eq_choices.product(extra_choices)
      indexes_for_choices(graph, count, sum, max, avg, group_by, choices, order_choices).uniq
    end

    def get_choices(graph, select, eq, range, group_by, entity_fields_patterns, extra_fields)
      eq_choices = frequent_eq_choices graph, group_by, entity_fields_patterns
      range_fields = graph.entities.map { |entity| range[entity] }.reduce(&:+).uniq
      order_choices = range_fields.prefixes.flat_map do |fields|
        fields.permutation.to_a
      end.uniq << []
      extra_choices = frequent_extra_choices(graph, select, eq, range, extra_fields)
      #puts("graph : #{graph.inspect}")
      puts("eq_choices : #{eq_choices.size}")
      puts("extra_choices : #{extra_choices.size}")

      [eq_choices, order_choices, extra_choices]
    end

    def get_frequent_entity_choices(queries)
      entity_fields = queries.map do |query|
        eq = get_query_eq query

        Hash[query,
             query.graph.subgraphs.flat_map do |subgraph|
               subgraph.entities.flat_map do |entity|
                 # Get the fields for the entity and add in the IDs
                 entity_field = eq[entity] << entity.id_field
                 entity_field.uniq!
                 1.upto(entity_field.count).flat_map do |n|
                   entity_field.permutation(n).to_a
                 end
               end
             end.uniq # how many fields the one query has does not matter.
        ]
      end.inject(:merge)

      unique_shared_fields = entity_fields.map do |q, efs_target|
        tmp_efs = efs_target.dup
        entity_fields.reject{|q_, _| q == q_}.each do |_, efs|
          efs_target -= efs
        end
        Hash[q, {unique: efs_target, shared: tmp_efs - efs_target}]
      end.inject(:merge)

      shared_fields = unique_shared_fields.map{|_, usf| usf[:shared]}
      support_threshold = [2, queries.size].min() # allow support == 1 if there is only one query
      frequent_shared_patterns = FpGrowth.mine(shared_fields)
                                   .select{|ef| ef.support >= support_threshold and ef.content.size > 1}

      unique_shared_fields = unique_shared_fields.map do |query, fields|
        current_frequent_shared_patterns = frequent_shared_patterns.select{|fsp| fields[:shared].to_set >= fsp.content.to_set}
        threshold = current_frequent_shared_patterns.size > 1 ? [current_frequent_shared_patterns.map{|cfs| cfs.support}.max - 1, 1].max : 1
        fields[:shared] = reduce_choices fields[:shared], current_frequent_shared_patterns, threshold
        Hash[query, fields]
      end.inject(:merge)
      unique_shared_fields
    end

    def get_frequent_extra_choices(queries)
      extras = queries.map do |query|
        eq = get_query_eq query
        range = get_query_eq query
        query.graph.subgraphs.flat_map do |subgraph|
          extra_choices subgraph, query.select, eq, range
        end
      end
      FpGrowth.mine(extras).select{|ef| ef.support > 1}
    end

    #候補数の母数を減らして，列挙数を増やせばより効率的にできるはず．列挙についてに，並列処理を入れる
    def frequent_eq_choices(graph, group_by, entity_fields_patterns)
      # filter not used fields for this sub-graph
      #entity_fields_patterns[:shared] = entity_fields_patterns[:shared].map{|efps| efps.select{|efs| efs.all?{|ef| graph.entities.include? ef.parent}}}
      shared_patterns = entity_fields_patterns[:shared].select{|efs| efs.all?{|ef| graph.entities.include? ef.parent}}
      unique_patterns = entity_fields_patterns[:unique].select{|efpu| efpu.all?{|efp| graph.entities.include? efp.parent}}

      # get each enumeration combination size for shared fields and unique fields
      graph_entities_size_for_shared = shared_patterns.flatten.map(&:parent).uniq.size
      graph_entities_size_for_unique = graph.entities.length - graph_entities_size_for_shared

      # unique fields is not shared with other queries and no need to enumerate subset of unique fields
      unique_eq_choices = 1.upto([graph_entities_size_for_unique, 1].max()).flat_map do |n|
        unique_patterns.combination(n)
                            .map(&:flatten)
                            .map(&:uniq)
                            .map{|e| e.sort_by { |b | [-b.cardinality, b.name] }}.uniq
      end << []

      # TODO: tmp
      unique_eq_choices = unique_eq_choices.map{|uec| sort_by_is_primary uec}

      # no need for enumerate for all entities because we add materialized view
      eq_choices = 1.upto(graph_entities_size_for_shared).flat_map do |n|
        #eq_choices = 1.upto(graph_entities_size_for_shared - 1).flat_map do |n|

        #shareds = entity_fields_patterns[:shared].flat_map{|efp| efp.permutation(n).to_a}
        shareds = shared_patterns.permutation(n).to_a
        # ここで候補を出し過ぎなのは間違いないが，ジョインプランが出るようにはしたい. share しているクエリ間で同じ分割方法をしていれば問題ないはず？ソートして partition をとるか
        #shareds = entity_fields_patterns[:shared].map{|efp| efp.map(&:to_set).uniq.map(&:to_a).partitions(n).to_a}.flatten(2).reject{|e| e.empty?}
        #shareds = entity_fields_patterns[:shared].map{|efp| efp.map(&:to_set).uniq.map(&:to_a).combination(n).map(&:flatten).map(&:uniq).uniq}
        #shareds = entity_fields_patterns[:shared].map{|efp| efp.map(&:to_set).uniq.map(&:to_a).combination(n).map{|rss| rss.sort_by { |rs | rs.map{|r| r.name}.uniq.join }}.map(&:flatten).map(&:uniq).uniq}
        (shareds.product(unique_eq_choices) + unique_eq_choices.product(shareds)).map{|f| f.flatten.uniq} + shareds.map(&:flatten)
      end + unique_eq_choices

      tmp = 1.upto(3).flat_map do |n|
        additional_eqs = (shared_patterns + unique_patterns).permutation(n).to_a.map(&:flatten).map(&:uniq).uniq
        additional_eqs.map{|aeps| sort_by_not_primary aeps}
      end
      eq_choices += tmp.uniq

      #unless group_by.empty?
      #  eq_choices = prune_eq_choices_for_groupby eq_choices, group_by
      #end

      eq_choices = eq_choices.uniq.reject{|ec| ec.empty?}
      valid_eq_choices = eq_choices.select{|eq_choice| graph.entities >= eq_choice.map{|ec| ec.parent}.to_set}
      valid_eq_choices
    end

    def frequent_extra_choices(graph, select, eq, range, extra_fields)
      extra_choices = extra_choices(graph, select, eq, range)
      current_extra_fields = extra_fields.select{|extra_field| extra_field.content.all?{|c| extra_choices.include? c}}
      frequent_extra_fields = extra_choices
      max_frequency = current_extra_fields.map(&:support).max()
      #frequent_extra_fields = reduce_choices(extra_choices, current_extra_fields, max_frequency).map{|c| c.flatten(1).uniq}
      extra_choices = 1.upto(frequent_extra_fields.length).flat_map do |n|
        frequent_extra_fields.combination(n).map{|fef| fef.flatten(1).uniq}
      end.map(&:to_set).uniq

      valid_extra_choices = extra_choices.select{|extra_choice| graph.entities >= extra_choice.map{|ec| ec.parent}.to_set}
      valid_extra_choices
    end

    # TODO: At least for extra_fields, reduce for all patterns reduces candidates too much. I need to used only effective sets
    # group choices if the choices are included in the same frequent pattern
    def reduce_choices(entity_choice, patterns, threshold)
      #patterns.select{|p| p.support >= threshold}.sort_by { |p| [-p.content.size, p.support]}.map(&:content).each do |pattern|
      #  if entity_choice.to_set >= pattern.to_set
      #    # ここで，一度実行すると配列のネストが深くなるので，二度目以降は実行されなくなる
      #    entity_choice = (entity_choice - pattern).map{|f| [f]} + [pattern]
      #  end
      #end
      frequent_fields_set = patterns.select{|p| p.support >= threshold}
                                .sort_by { |p| [-p.content.size, p.support]}
                                .map(&:content)
                                .first
      #return [entity_choice] if frequent_fields_set.nil?
      #entity_choice = (entity_choice - frequent_fields_set).map{|f| [f]} + [sort_by_is_primary(frequent_fields_set.flatten.uniq)]

      return entity_choice if frequent_fields_set.nil?
      entity_choice = (entity_choice - frequent_fields_set) + [sort_by_is_primary(frequent_fields_set.flatten.uniq)]

      entity_choice
    end

    def prune_eq_choices_for_groupby(eq_choices, group_by)
      # remove eq_choices that does not have group by fields
      eq_choices_with_groupby = eq_choices.select{|ec| ec.to_set >= group_by}
      eq_choices_without_groupby = eq_choices - eq_choices_with_groupby
      eq_choices_with_groupby = eq_choices_with_groupby.map do |eq_choice|
        eq_choice.reject!{|ec| group_by.include? ec}

        # force eq_choice to start from group by fields
        group_by.sort_by { |gb| gb.name } + eq_choice
      end

      eq_choices_with_groupby + eq_choices_without_groupby
    end

    def get_median(a)
      a.sort!
      (a.size % 2).zero? ? a[a.size/2 - 1, 2].inject(:+) / 2.0 : a[a.size/2]
    end

    def sort_by_is_primary(fields)
      primary_fields = fields.select{|f| f.primary_key?}.sort_by { |fp| [-fp.cardinality, fp.name]}
      non_primary_fields = fields.select{|f| !f.primary_key?}.sort_by { |fp| [-fp.cardinality, fp.name]}
      primary_fields + non_primary_fields
    end

    def sort_by_not_primary(fields)
      primary_fields = fields.select{|f| f.primary_key?}.sort_by { |fp| [-fp.cardinality, fp.name]}
      non_primary_fields = fields.select{|f| !f.primary_key?}.sort_by { |fp| [-fp.cardinality, fp.name]}
      non_primary_fields + primary_fields
    end

    def entity_field_combinations(entity_fields)
      return [] if entity_fields.size == 0
      entity_hash = {}
      entity_fields.reject{|efs| efs.empty?}.each do |efs|
        fail 'each entity fields should have only one entity' if efs.map(&:parent).uniq.size > 1
        unless entity_hash.has_key? efs.first.parent
          entity_hash[efs.first.parent] = []
          entity_hash[efs.first.parent] << []
        end
        entity_hash[efs.first.parent] << efs
      end
      entity_hash.values[0].product(*entity_hash.values[1..-1])
    end
  end
end

