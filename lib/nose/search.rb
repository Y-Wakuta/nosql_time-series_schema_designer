# frozen_string_literal: true

require_relative 'search/constraints'
require_relative 'search/time_depend_constraints'
require_relative 'search/problem'
require_relative 'search/time_depend_problem'
require_relative 'search/results'
require_relative 'search/time_depend_results'
require_relative 'plans/time_depend_plan'

require 'logging'
require 'ostruct'
require 'tempfile'
require 'parallel'
require 'etc'

module NoSE
  # ILP construction and schema search
  module Search
    # Searches for the optimal indices for a given workload
    class Search
      def initialize(workload, cost_model, objective = Objective::COST,
                     by_id_graph = false, is_pruned = true)
        @logger = Logging.logger['nose::search']
        @workload = workload
        @cost_model = cost_model
        @objective = objective
        @by_id_graph = by_id_graph
        @is_pruned = is_pruned

        # For now we only support optimization based on cost when grouping by
        # ID graphs, but support for other objectives is still feasible
        fail 'Only cost-based optimization allowed when using ID graphs' \
          if @by_id_graph && objective != Objective::COST
      end

      # Search for optimal indices using an ILP which searches for
      # non-overlapping indices
      # @return [Results]
      def search_overlap(indexes, max_space = Float::INFINITY)
        return if indexes.empty?
        STDERR.puts("set basic query plans")

        # Get the costs of all queries and updates
        query_weights = combine_query_weights indexes
        RunningTimeLogger.info(RunningTimeLogger::Headers::START_QUERY_PLAN_ENUMERATION)
        costs, trees = query_costs query_weights, indexes
        RunningTimeLogger.info(RunningTimeLogger::Headers::END_QUERY_PLAN_ENUMERATION)

        show_tree trees

        # prepare_update_costs possibly takes nil value if the workload is not time-depend workload
        update_costs, update_plans, prepare_update_costs = update_costs trees, indexes

        log_search_start costs, query_weights

        solver_params = {
            max_space: max_space,
            costs: costs,
            update_costs: update_costs,
            prepare_update_costs: prepare_update_costs,
            cost_model: @cost_model,
            by_id_graph: @by_id_graph,
            trees: trees
        }

        if @workload.is_a? TimeDependWorkload
          RunningTimeLogger.info(RunningTimeLogger::Headers::START_MIGRATION_PLAN_ENUMERATION)
          STDERR.puts("set migration query plans")

          if @workload.is_static || @workload.is_first_ts || @workload.is_last_ts
            STDERR.puts "Since the workload is static, not enumerating the migrate prepare plans"
            migrate_prepare_plans = {}
          else
            starting = Time.now.utc
            migrate_prepare_plans = get_migrate_preparing_plans(trees, indexes)
            STDERR.puts "prepare plan enumeration time: " + (Time.now.utc - starting).to_s
            migrate_prepare_plans = {} if migrate_prepare_plans.values.size == 1 && migrate_prepare_plans.values.first.empty?

            costs.merge!(migrate_prepare_plans.values.flat_map{|v| v.values}.map{|v| v[:costs]}.reduce(&:merge) || {})
          end

          solver_params[:migrate_prepare_plans] = migrate_prepare_plans
          RunningTimeLogger.info(RunningTimeLogger::Headers::END_MIGRATION_PLAN_ENUMERATION)
        end

        search_result query_weights, indexes, solver_params, trees,
                      update_plans
      end

      def pruning_indexes_by_plan_cost(indexes)
        query_weights = combine_query_weights indexes
        _, trees = query_costs query_weights, indexes
        before_index_size = indexes.size
        planner = @is_pruned ?
                      Plans::PrunedQueryPlanner.new(@workload, indexes, @cost_model, 2) :
                      Plans::QueryPlanner.new(@workload, indexes, @cost_model)

        plan_num_threshold = 2
        cost_ratio_threshold = 10_000
        trees.select{|t| t.to_a.size >= plan_num_threshold}.each do |tree|
          before_tree_size = tree.size
          tree_max_cost_threshold = tree.map(&:cost).min * cost_ratio_threshold
          tree.each do |plan|
            if plan.cost > tree_max_cost_threshold
              planner.prune_plan(plan.last)
            end
          end
          after_tree_size = tree.size
          STDERR.puts "cost-base pruned plans: #{before_tree_size} -> #{after_tree_size}, #{tree.query.text}" if before_tree_size > after_tree_size
        end
        STDERR.puts "cost-base pruned indexes: #{before_index_size} -> #{trees.flat_map{|t| t.flat_map(&:indexes)}.uniq.size}"
        trees.flat_map{|t| t.flat_map(&:indexes)}.uniq
      end

      private

      def show_tree(trees)
        trees.each do |tree|
          join_plan_size = tree.select{|p| p.indexes.size > 1}.size
          puts "--- #{tree.to_a.size} plans : #{join_plan_size} join plans for #{tree.query.text}  ---" if tree.query.instance_of? Query
        end
      end

      # Combine the weights of queries and statements
      # @return [void]
      def combine_query_weights(indexes)
        indexes = indexes.map(&:to_id_graph).uniq if @by_id_graph
        query_weights = Hash[@workload.support_queries(indexes).map do |query|
          [query, @workload.statement_weights[query.statement]]
        end]
        query_weights.merge!(@workload.statement_weights.select do |stmt, _|
          stmt.is_a? Query
        end.to_h)

        query_weights
      end

      # Produce a useful log message before starting the search
      # @return [void]
      def log_search_start(costs, query_weights)
        @logger.debug do
          "Costs: \n" + pp_s(costs) + "\n" \
            "Search with queries:\n" + \
            query_weights.each_key.map.with_index do |query, i|
            "#{i} #{query.inspect}"
          end.join("\n")
        end
      end

      # Run the solver and get the results of search
      # @return [Results]
      def search_result(query_weights, indexes, solver_params, trees,
                        update_plans)
        # Solve the LP using MIPPeR
        STDERR.puts "start optimization : #{Time.now}"
        result = solve_mipper query_weights.keys, indexes, update_plans, **solver_params

        setup_result result, solver_params, update_plans
        result
      end

      def setup_result(result, solver_params, update_plans)
        result.workload = @workload
        result.plans_from_trees solver_params[:trees]
        result.set_update_plans update_plans
        result.cost_model = @cost_model

        if result.is_a? TimeDependResults
          STDERR.puts "set migration plans"
          result.set_time_depend_plans
          result.set_time_depend_indexes
          result.set_time_depend_update_plans
          result.set_migrate_preparing_plans solver_params[:migrate_prepare_plans] unless solver_params[:migrate_prepare_plans].nil?
          result.calculate_cost_each_timestep
        end
        result.validate
        result
      end

      # Select the plans to use for a given set of indexes
      # @return [Array<Plans::QueryPlan>]
      def select_plans(trees, indexes)
        trees.map do |tree|
          # Exclude support queries since they will be in update plans
          query = tree.query
          next if query.is_a?(SupportQuery)

          # Select the exact plan to use for these indexes
          tree.select_using_indexes(indexes).min_by(&:cost)
        end.compact
      end

      # Solve the index selection problem using MIPPeR
      # @return [Results]
      def solve_mipper(queries, indexes, update_plans, data)
        # Construct and solve the ILP
        problem = @workload.is_a?(TimeDependWorkload) ?
                      TimeDependProblem.new(queries, @workload, data, @objective)
                      : Problem.new(queries, @workload.updates, data, @objective)

        RunningTimeLogger.info(RunningTimeLogger::Headers::START_WHOLE_OPTIMIZATION)
        problem.solve
        RunningTimeLogger.info(RunningTimeLogger::Headers::END_WHOLE_OPTIMIZATION)

        # We won't get here if there's no valdi solution
        @logger.debug 'Found solution with total cost ' \
                      "#{problem.objective_value}"

        # Return the selected indices
        selected_indexes = problem.selected_indexes

        @logger.debug do
          "Selected indexes:\n" + selected_indexes.map do |index|
            "#{indexes.index index} #{index.inspect}"
          end.join("\n")
        end

        problem.result
      end

      # Produce the cost of updates in the workload
      def update_costs(trees, indexes)
        planner = Plans::UpdatePlanner.new @workload.model, trees, @cost_model,
                                           @by_id_graph
        update_costs = Hash.new { |h, k| h[k] = {} }
        update_plans = Hash.new { |h, k| h[k] = [] }
        update_statements = @workload.statements.reject{|statement| statement.is_a? Query}

        if @workload.is_a?(TimeDependWorkload)
          prepare_update_costs = Hash.new { |h, k| h[k] = {}}
          update_statements.each do |statement|
            populate_update_costs planner, statement, indexes,
                                  update_costs, update_plans, prepare_update_costs
          end
          return [update_costs, update_plans, prepare_update_costs]
        else
          update_statements.each do |statement|
            populate_update_costs planner, statement, indexes,
                                  update_costs, update_plans
          end
          return [update_costs, update_plans]
        end
      end

      # Populate the cost of all necessary plans for the given statement
      # @return [void]
      def populate_update_costs(planner, statement, indexes,
                                update_costs, update_plans, preparing_update_costs = nil)
        plans_for_update = planner.find_plans_for_update(statement, indexes)
        weights = @workload.statement_weights[statement]
        plans_for_update.each do |plan|
          if @workload.is_a?(TimeDependWorkload)
            update_costs[statement][plan.index] = weights.map{|w| plan.update_cost * w}
            plan.steps.each { |step| step.calculate_update_prepare_cost @cost_model }

            # the definition of query frequency is execution times per second.
            # But the weight is multiplied frequency by migration interval,
            # Therefore, divide this value by interval to get query frequency
            preparing_update_costs[statement][plan.index] =
              weights.map{|w| (plan.prepare_update_cost_with_size / @workload.interval) * w}
          else
            update_costs[statement][plan.index] = plan.update_cost * weights
          end
          update_plans[statement] << plan
        end
      end

      # Get the cost of using each index for each query in a workload
      def query_costs(query_weights, indexes)
        planner = @is_pruned ?
                      Plans::PrunedQueryPlanner.new(@workload, indexes, @cost_model, 2) :
                      Plans::QueryPlanner.new(@workload, indexes, @cost_model)

        results = Parallel.map(query_weights, in_processes: [Parallel.processor_count - 4, 0].max()) do |query, weight|
          query_cost planner, query, weight
        end
        costs = Hash[query_weights.each_key.map.with_index do |query, q|
          [query, results[q].first]
        end]

        [costs, results.map(&:last)]
      end

      # get candidate indexes for each index.
      # not migrating mv plan to mv plan
      def get_migrate_preparing_plans_by_trees(trees, indexes)
        puts "start gathering index candidates for index #{Time.now}"

        decomposed_candidate_indexes = Parallel.map(trees.select{|t| t.query.instance_of? Query},
                                         in_processes: [Parallel.processor_count / 2, Parallel.processor_count - 5].max()) do |target_tree|
          candidates = {}
          related_trees = trees.select{|qt| not (qt.query.graph.entities & target_tree.query.graph.entities).empty?}
          target_tree.each do |target_plan|
            if target_plan.indexes.size > 1
              target_plan.indexes.each do |target_idx|
                candidates[target_idx] = Set.new unless candidates.has_key? target_idx
                candidates[target_idx] = indexes
                                           .reject{|i| i == target_idx || \
                                                                  is_similar_index?(target_idx, i) || \
                                                                  (i.graph.entities & target_idx.graph.entities).empty?
                                           }.to_set
              end
            else
              target_plan.indexes.each do |target_idx|
                candidates[target_idx] = Set.new unless candidates.has_key? target_idx
                other_indexes = related_trees.flat_map(&:to_a)
                                             .reject{|p| p == target_plan || p.indexes.size == 1}
                                             .flat_map(&:indexes).uniq
                candidates[target_idx] += other_indexes
                                                    .reject{|i| i == target_idx || \
                                                                is_similar_index?(target_idx, i) || \
                                                                (i.graph.entities & target_idx.graph.entities).empty?
                                                    }.to_set
              end
            end
          end
          candidates
        end
        candidate_indexes = {}
        indexes.each {|idx| candidate_indexes[idx] = Set.new}
        decomposed_candidate_indexes.each {|ci| ci.each {|k, v| candidate_indexes[k] += v}}
        puts "end gathering index candidates for index #{Time.now}"
        candidate_indexes
      end

      def get_migrate_preparing_plans(query_trees, indexes)
        query_indexes = query_trees.select{|t| t.query.instance_of? Query}.flat_map{|t| t.flat_map(&:indexes)}.uniq
        puts "index size: #{indexes.size}, query_index size: #{query_indexes.size}"
        candidate_indexes = get_migrate_preparing_plans_by_trees query_trees, indexes
        index_related_tree_hash = get_related_query_tree(query_trees, query_indexes)
        # create new migrate_prepare_plan
        migrate_plans = Parallel.map(query_indexes, in_processes: [Parallel.processor_count / 4, Parallel.processor_count - 5].max()) do |base_index|
          usable_indexes = candidate_indexes[base_index]

          m_plan = {base_index => {}}
          planner = Plans::MigrateSupportSimpleQueryPlanner.new @workload, usable_indexes, @cost_model, 2
          migrate_support_query = MigrateSupportQuery.migrate_support_query_for_index(base_index)
          begin
            m_plan[base_index][migrate_support_query] = support_query_cost migrate_support_query, planner
          rescue Plans::NoPlanException => e
            #puts "#{e.inspect} for #{base_index.key}"
          end

          # convert existing other trees into migrate_prepare_tree
          index_related_tree_hash[base_index].each do |rtree|
            simple_query = MigrateSupportSimplifiedQuery.simple_query rtree.query, base_index
            next if simple_query.text == migrate_support_query.text

            # only use indexes in related tree
            current_rtree_indexes = (rtree.flat_map{|r| r.indexes}.uniq.to_set & usable_indexes.to_set)
            planner = Plans::MigrateSupportSimpleQueryPlanner.new @workload, current_rtree_indexes, @cost_model, 2
            begin
              cost_tree = support_query_cost simple_query, planner,
                                             existing_tree: m_plan[base_index].has_key?(migrate_support_query) ?
                                                              m_plan[base_index][migrate_support_query][:tree] : nil
            rescue Plans::NoPlanException => e
              next
            end
            m_plan[base_index][simple_query] = cost_tree
          end
          m_plan
        end.reduce(&:merge)
        migrate_plans
      end

      def get_related_query_tree(trees, indexes)
        related_tree_hash = {}
        indexes.each {|qi| related_tree_hash[qi] = Set.new}
        trees.select{|t| t.query.instance_of? Query}.each do |t|
          t.flat_map(&:indexes).uniq.each {|i| related_tree_hash[i].add(t)}
        end
        related_tree_hash
      end

      def is_similar_index?(index, other_index)
        # e.g. index `[f1, f2][f3, f4] -> [f5, f6]`

        # in the case that hash_fields are same and all fields in the column family is also same
        return true if other_index.hash_fields == index.hash_fields && other_index.all_fields.to_set == index.all_fields.to_set

        # if the key fields are same, small field difference can be allowed
        return true if other_index.hash_fields == index.hash_fields && \
                       other_index.order_fields == index.order_fields && \
                       other_index.extra >= index.extra && \
                       (other_index.extra - index.extra).size < 2

        # #return true if index.hash_fields > other_index.hash_fields and \
        # #      (index.hash_fields + index.order_fields.to_set) \
        # #        <= (other_index.hash_fields + other_index.order_fields.to_set)
        # return true if other_index.key_fields.to_set >= index.key_fields.to_set and \
        #                (other_index.all_fields - index.all_fields).size < 3

        # # similar: other_index `[f1][f2, f3] -> [f4, f5, f6]`
        # # similar: other_index `[f1][f2, f3] -> [f4, f5, f6, f7, f8]`
        # return true if index.hash_fields >= other_index.hash_fields and \
        #                index.order_fields.to_set >= index.hash_fields - other_index.hash_fields

        false
      end

      def support_query_cost_4_costs_tree(query, costs, tree)
        # validate support query tree
        tree.each do |plan|
          if plan.steps.map{|s| s.index.all_fields}.reduce(&:+) < query.index.all_fields
            lacked_fields = plan.steps.map{|s| s.index.all_fields}.reduce(&:+) - query.select
            fail "ms_support query does not get required fields : " + lacked_fields.inspect
          end
        end

        costs = Hash[query, costs]
        {:costs => costs, :tree => tree}
      end

      def support_query_cost(query, planner, existing_tree: nil)
        tree = planner.find_plans_for_query(query)
        remove_already_existing_plan planner, tree, existing_tree unless existing_tree.nil?

        _costs, tree = query_cost_4_tree query, [1] * @workload.timesteps, tree
        support_query_cost_4_costs_tree query, _costs, tree
      end

      def remove_already_existing_plan(planner, target_tree, existing_tree)
        target_tree.each do |plan|
          existing_plans = existing_tree.to_a
          is_already_existed = existing_plans.any? do |existing_plan|
            existing_plan.indexes == plan.indexes
          end
          is_empty_tree = planner.prune_plan plan.steps.last if is_already_existed
          fail Plans::NoPlanException if is_empty_tree
        end
      end

      def query_cost_4_tree(query, weight, tree)
        query_costs = {}
        tree.each do |plan|
          steps_by_index = []
          plan.each do |step|
            if step.is_a? Plans::IndexLookupPlanStep
              steps_by_index.push [step]
            else
              steps_by_index.last.push step
            end
          end

          populate_query_costs query_costs, steps_by_index, weight, query, tree
        end

        [query_costs, tree]
      end

      # Get the cost for indices for an individual query
      def query_cost(planner, query, weight)
        tree = planner.find_plans_for_query(query)
        query_cost_4_tree query, weight, tree
      end

      # Store the costs and indexes for this plan in a nested hash
      # @return [void]
      def populate_query_costs(query_costs, steps_by_index, weight,
                               query, tree)
        # The first key is the query and the second is the index
        #
        # The value is a two-element array with the indices which are
        # jointly used to answer a step in the query plan along with
        # the cost of all plan steps for the part of the query graph
        steps_by_index.each do |steps|
          # Get the indexes for these plan steps
          index_step = steps.first

          # Calculate the cost for just these steps in the plan
          cost = weight.is_a?(Array) ? weight.map{|w| steps.sum_by(&:cost) * w}
                   : steps.sum_by(&:cost) * weight

          # Don't count the cost for sorting at the end
          sort_step = steps.find { |s| s.is_a? Plans::SortPlanStep }
          unless sort_step.nil?
            weight.is_a?(Array) ? weight.map.with_index{|w, i| cost[i] -= sort_step.cost * w}
              : (cost -= sort_step.cost * weight)
          end

          if query_costs.key? index_step.index
            current_cost = query_costs[index_step.index].last

            unless is_same_cost current_cost, cost
              if is_larger_cost current_cost, cost
                # if the new cost value is bigger than the current value,
                # delete newly created query plan
                planner = Plans::QueryPlanner.new(@workload, nil, @cost_model)
                planner.prune_plan index_step
              else
                # if the new cost value is smaller than the current value,
                # overwrite costly plan and update the cost value
                query_costs[index_step.index] = [steps, cost]
              end
              next
            end

            # We must always have the same cost
            # WARNING: fix this invalid conditions.
            # Ignoring steps that have filtering steps just overwrites the cost value of step in another query plan.
            if !is_same_cost(current_cost, cost) \
              && !has_parent_filter_step(steps.last) \
              && !has_parent_filter_step(query_costs[index_step.index].first.last)
              index = index_step.index
              p query.class
              p query
              puts "Index #{index.key} does not have equivalent cost"
              puts "Current cost: #{current_cost}, discovered cost: #{cost}"

              puts "\nCurrent steps"
              query_costs[index_step.index].first.each { |s| p s }

              puts "\nDiscovered steps"
              steps.each { |s| p s }
              puts

              puts '======================================='
              tree.sort_by(&:cost).each do |plan|
                next unless plan.indexes.include?(index_step.index)
                plan.each do |step|
                  print(format('%.3f', step.cost).rjust(7) + ' ')
                  p step
                end
                puts "#{format('%.3f', plan.cost).rjust(7)} total"
                puts '======================================='
              end

              #puts
              #p tree

              #fail
            end
          else
            # We either found a new plan or something cheaper
            query_costs[index_step.index] = [steps, cost]
          end
        end
      end

      def has_parent_filter_step(step)
        current = step
        while true
          return false if current.is_a? Plans::RootPlanStep
          return true if current.is_a? Plans::FilterPlanStep
          current = current.parent
        end
        fail
      end

      def is_same_cost(cost1, cost2)
        if cost1.is_a?(Array) && cost2.is_a?(Array)
          cost1.each_with_index do |c1,i|
            return false unless (cost2[i] - c1).abs < 0.001
          end
          return true
        else
          return (cost1 - cost2).abs < 0.001
        end
      end

      def is_larger_cost(cost1, cost2)
        if cost1.is_a?(Array) && cost2.is_a?(Array)
          cost1.each_with_index do |c1,i|
            return false unless c1 < cost2[i]
          end
        else
          return cost1 < cost2
        end
      end
    end
  end
end
