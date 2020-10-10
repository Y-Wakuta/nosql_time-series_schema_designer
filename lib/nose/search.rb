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
                     by_id_graph = false, is_pruned = false)
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

        indexes = pruning_tree_by_cost indexes
        # Get the costs of all queries and updates
        query_weights = combine_query_weights indexes
        costs, trees = query_costs query_weights, indexes

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
          STDERR.puts("set migration query plans")

          migrate_prepare_plans = get_migrate_preparing_plans(trees, indexes)
          costs.merge!(migrate_prepare_plans.values.flat_map{|v| v.values}.map{|v| v[:costs]}.reduce(&:merge))

          solver_params[:migrate_prepare_plans] = migrate_prepare_plans
        end

        search_result query_weights, indexes, solver_params, trees,
                      update_plans
      end

      private

      def pruning_tree_by_cost(indexes)
        query_weights = combine_query_weights indexes
        _, trees = query_costs query_weights, indexes
        puts "index size before cost-based pruning: #{indexes.size}"
        planner = @is_pruned ?
                      Plans::PrunedQueryPlanner.new(@workload, indexes, @cost_model, 2) :
                      Plans::QueryPlanner.new(@workload, indexes, @cost_model)

        threshold = 1
        trees.select{|t| t.to_a.size > threshold}.each do |tree|
          before_tree_size = tree.size
          plan_costs = tree.map(&:cost)
          cost_threshold = 0.1 * (plan_costs.max() - plan_costs.min()) + plan_costs.min()
          tree.each do |plan|
            if plan.cost > cost_threshold
              planner.prune_plan(plan.last)
            end
          end
          after_tree_size = tree.size
          puts "pruned #{before_tree_size} -> #{after_tree_size}, #{tree.query.text}" if before_tree_size > after_tree_size
          puts "all plan cost was the same value #{tree.query.text}" if tree.all?{|p| p.indexes == 1} and plan_costs.size > 1 and plan_costs.uniq.size == 1
        end
        puts "index size after cost-based pruning: #{indexes.size}"
        trees.flat_map{|t| t.flat_map(&:indexes)}.uniq
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
        STDERR.puts "start optimization"
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
          result.calculate_cost_each_timestep
          result.set_time_depend_plans
          result.set_time_depend_indexes
          result.set_time_depend_update_plans
          result.set_migrate_preparing_plans solver_params[:migrate_prepare_plans]
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

        STDERR.puts "start solving"
        problem.solve
        STDERR.puts "solving is done"

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

        results = Parallel.map(query_weights, in_processes: 6) do |query, weight|
          query_cost planner, query, weight
        end
        costs = Hash[query_weights.each_key.map.with_index do |query, q|
          [query, results[q].first]
        end]

        [costs, results.map(&:last)]
      end

      def get_migrate_preparing_plans(trees, indexes)

        # create new migrate_prepare_plan
        migrate_plans = Parallel.map(indexes.uniq, in_processes: Etc.nprocessors - 2) do |base_index|

          #useable_indexes = indexes
          useable_indexes = indexes.reject{|oi| is_similar_index?(base_index, oi)}
          useable_indexes << base_index
          #puts "basic index size: #{indexes.size}, useable_index size: #{useable_indexes.size}"

          # tmp ====
          #puts "look for similar index"
          #puts base_index.hash_str
          #indexes.each do |other_index|
          #  if is_similar_index? base_index, other_index
          #    puts "  " + other_index.hash_str
          #  end
          #end
          # tmp ====

          m_plan = {base_index => {}}
          planner = Plans::PreparingQueryPlanner.new @workload, useable_indexes, @cost_model, base_index,  2
          migrate_support_query = MigrateSupportQuery.migrate_support_query_for_index(base_index)
          m_plan[base_index][migrate_support_query] = support_query_cost migrate_support_query, planner

          # convert existing other trees into migrate_prepare_tree
          #related_trees = trees.select{|t| t.query.instance_of? Query}.select{|t| t.flat_map{|p| p.indexes}.include? base_index}
          related_trees = trees.select{|t| t.flat_map{|p| p.indexes}.include? base_index}
          related_trees.each do |rtree|
            simple_query = MigrateSupportSimplifiedQuery.simple_query rtree.query, base_index
            planner = Plans::MigrateSupportSimpleQueryPlanner.new @workload, indexes.reject{|i| i==base_index}, @cost_model,  2
            begin
              m_plan[base_index][simple_query] = support_query_cost simple_query, planner
            rescue Plans::NoPlanException => e
              puts "#{e.inspect} for #{base_index.key}"
              next
            end
          end
          m_plan
        end.reduce(&:merge)
        migrate_plans
      end

      def is_similar_index?(index, other_index)
        # e.g. `[f1, f2][f3, f4] -> [f5, f6]`

        # similar: `[f1, f2][f3] -> [f4, f5, f6]`
        return true if index.hash_fields == other_index.hash_fields and index.all_fields == other_index.all_fields

        # similar: `[f1, f2][f3] -> [f4, f5, f6, f7, f8]`
        return true if index.hash_fields == other_index.hash_fields and \
          other_index.all_fields > index.all_fields and other_index.all_fields.size - index.all_fields.size < 3

        false
      end

      def support_query_cost(query, planner)
        _costs, tree = query_cost planner, query, [1] * @workload.timesteps

        # calculate cost
        _costs = _costs.map do |index, (step, costs)|
          # query execution cost already considers record width.
          # Therefore the cost is multiplied by index.entries not by index.size
          {index =>  [step, costs.map{|cost| @workload.migrate_support_coeff * cost * index.entries}]}
        end.reduce(&:merge)

        costs = Hash[query, _costs]
        {:costs => costs, :tree => tree}
      end

      # Get the cost for indices for an individual query
      def query_cost(planner, query, weight)
        query_costs = {}

        tree = planner.find_plans_for_query(query)
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

      def is_same_cost(cost1, cost2)
        if cost1.is_a?(Array) and cost2.is_a?(Array)
          cost1.each_with_index do |c1,i|
            return false unless (cost2[i] - c1).abs < 0.001
          end
          return true
        else
          return (cost1 - cost2).abs < 0.001
        end
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

            # We must always have the same cost
            # WARNING: fix this invalid conditions.
            # Ignoring steps that have filtering steps just overwrites the cost value of step in another query plan.
            if not is_same_cost(current_cost, cost) \
              and not has_parent_filter_step(steps.last) \
              and not has_parent_filter_step(query_costs[index_step.index].first.last)
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

              fail
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
    end
  end
end
