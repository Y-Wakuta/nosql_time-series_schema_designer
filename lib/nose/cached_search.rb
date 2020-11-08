# frozen_string_literal: true

require_relative 'search/constraints'
require_relative 'search/time_depend_constraints'
require_relative 'search/time_depend_iterative_constraints'
require_relative 'search/problem'
require_relative 'search/time_depend_problem'
require_relative 'search/time_depend_iterative_problem'
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
    class CachedSearch < IterativeSearch

      def search_overlap(indexes, query_trees, migrate_support_query_trees, max_space = Float::INFINITY)
        return if indexes.empty?
        STDERR.puts("set basic query plans : cached")

        # Get the costs of all queries and updates
        query_weights = combine_query_weights indexes
        costs, trees = query_costs_by_trees query_weights, query_trees

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
          STDERR.puts("set migration query plans : cached")

          starting = Time.now.utc
          migrate_prepare_plans = get_migrate_preparing_plans(trees, indexes, migrate_support_query_trees)
          ending = Time.now.utc
          STDERR.puts "prepare plan enumeration time: " + (ending - starting).to_s
          costs.merge!(migrate_prepare_plans.values.flat_map{|v| v.values}.map{|v| v[:costs]}.reduce(&:merge))

          solver_params[:migrate_prepare_plans] = migrate_prepare_plans
        end

        search_result query_weights, indexes, solver_params, trees,
                      update_plans
      end

      def get_query_trees_hash(indexes)
        query_weights = combine_query_weights indexes
        _, trees = query_costs query_weights, indexes
        trees.map{|t| Hash[t.query, t]}.reduce(&:merge)
      end

      def get_support_query_trees_hash(query_trees, indexes)
        index_related_tree_hash = get_related_query_tree(query_trees.values, indexes)

        ## create new migrate_prepare_plan
        migrate_plans = Parallel.map(indexes.uniq, in_processes: Etc.nprocessors / 2) do |base_index|

          useable_indexes = indexes.reject{|oi| is_similar_index?(base_index, oi)}
          useable_indexes << base_index

          m_plan = {base_index => {}}
          planner = Plans::PreparingQueryPlanner.new @workload, useable_indexes, @cost_model, base_index,  2
          migrate_support_query = MigrateSupportQuery.migrate_support_query_for_index(base_index)
          m_plan[base_index][migrate_support_query] = support_query_cost(migrate_support_query, planner)[:tree]

          # convert existing other trees into migrate_prepare_tree
          index_related_tree_hash[base_index].each do |rtree|
            simple_query = MigrateSupportSimplifiedQuery.simple_query rtree.query, base_index
            planner = Plans::MigrateSupportSimpleQueryPlanner.new @workload, indexes.reject{|i| i==base_index}, @cost_model,  2
            begin
              m_plan[base_index][simple_query] = support_query_cost(simple_query, planner)[:tree]
            rescue Plans::NoPlanException => e
              #puts "#{e.inspect} for #{base_index.key}"
              next
            end
          end
          m_plan
        end.reduce(&:merge)
        migrate_plans
      end

      # migrate_support_query_trees は  base_index, support_query の２階層 hash
      def get_migrate_preparing_plans(query_trees, indexes, migrate_support_query_trees)
        index_related_tree_hash = get_related_query_tree(query_trees, indexes)

        ## create new migrate_prepare_plan
        migrate_plans = Parallel.map(indexes.uniq, in_processes: Etc.nprocessors / 2) do |base_index|

          m_plan = {base_index => {}}
          migrate_support_query = MigrateSupportQuery.migrate_support_query_for_index(base_index)
          m_plan[base_index][migrate_support_query] =
              support_query_cost_by_trees migrate_support_query, migrate_support_query_trees[base_index][migrate_support_query]

          # convert existing other trees into migrate_prepare_tree
          index_related_tree_hash[base_index].each do |rtree|
            simple_query = MigrateSupportSimplifiedQuery.simple_query rtree.query, base_index
            unless migrate_support_query_trees[base_index][simple_query].nil?
              begin
                m_plan[base_index][simple_query] =
                    support_query_cost_by_trees simple_query, migrate_support_query_trees[base_index][simple_query]
              rescue Plans::NoPlanException
                next
              end
            end
          end
          m_plan
        end.reduce(&:merge)
        migrate_plans
      end

      def support_query_cost_by_trees(query, tree)
        _costs, tree = query_cost_4_tree query, [1] * @workload.timesteps, tree

        support_query_cost_4_costs_tree query, _costs, tree
      end

      # Get the cost of using each index for each query in a workload
      def query_costs_by_trees(query_weights, trees)

        results = Parallel.map(query_weights, in_processes: Etc.nprocessors - 4) do |query, weight|
          query_cost_4_tree query, weight, trees.select{|q, t| q == query}.values.first
        end
        costs = Hash[query_weights.each_key.map.with_index do |query, q|
          [query, results[q].first]
        end]

        [costs, results.map(&:last)]
      end

    end
  end
end
