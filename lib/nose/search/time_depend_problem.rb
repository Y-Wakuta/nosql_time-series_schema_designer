# frozen_string_literal: true

require 'logging'

require 'mipper'
begin
  require 'mipper/cbc'
rescue LoadError
  # We can't use most search functionality, but it won't explode
  nil
end

module NoSE
  module Search
    # A representation of a search problem as an ILP
    class TimeDependProblem < Problem
      attr_reader :timesteps, :migrate_vars, :prepare_vars, :trees,
                  :creation_coeff, :migrate_support_coeff,
                  :migrate_prepare_plans, :prepare_tree_vars, :is_static

      def initialize(queries, workload, data, objective = Objective::COST)
        fail if workload.timesteps.nil?

        @timesteps = workload.timesteps
        @creation_coeff = workload.creation_coeff
        @migrate_support_coeff = workload.migrate_support_coeff
        @trees = data[:trees]
        @migrate_prepare_plans = data[:migrate_prepare_plans]
        migrate_support_queries = @migrate_prepare_plans.values.flat_map(&:keys)
        queries += migrate_support_queries
        @include_migration_cost = workload.include_migration_cost
        @MIGRATE_COST_DUMMY_CONST = 0.00001 # multiple migrate cost by this value to ignore
        @is_static = workload.is_static or workload.is_first_ts or workload.is_last_ts

        super(queries, workload.updates, data, objective)
      end

      def add_migration_cost(cost)
        cost = add_migrate_update_costs cost
        cost = add_creation_cost cost
        cost = add_prepare_cost cost
        cost
      end

      # Get the cost of all queries in the workload
      # @return [MIPPeR::LinExpr]
      def total_cost
        cost = @queries.reject{|q| q.is_a? MigrateSupportQuery}
                   .reduce(MIPPeR::LinExpr.new) do |expr, query|
          used_indexes = data[:costs][query].keys
          expr.add(used_indexes.reduce(MIPPeR::LinExpr.new) do |subexpr, index|
            subexpr.add((0...@timesteps).reduce(MIPPeR::LinExpr.new) do |subsubexpr, ts|
              subsubexpr.add total_query_cost(@data[:costs][query][index],
                                              @query_vars[index][query],
                                              @sort_costs[query][index],
                                              @sort_vars[query][index],
                                              ts)
            end)
          end)
        end

        cost = add_update_costs cost
        cost = add_migration_cost(cost)
        cost = add_initial_index_loading_cost(cost)
        cost
      end

      # index which is during the preparing for the next timestep also need to be updated
      def add_migrate_update_costs(min_cost)
        @updates.each do |update|
          @indexes.each do |index|
            index = index.to_id_graph if data[:by_id_graph]
            next unless update.modifies_index?(index)

            # index which is during the preparing for the next timestep also need to be updated
            (0...(@timesteps - 1)).each do |ts|
              cost = @include_migration_cost ? @data[:prepare_update_costs][update][index][ts]
                         : @MIGRATE_COST_DUMMY_CONST

              min_cost.add @migrate_vars[index][ts + 1] * cost
            end
          end
        end
        min_cost
      end

      # Deal with updates which do not require support queries
      # @return [MIPPeR::LinExpr]
      def add_update_costs(min_cost)
        @updates.each do |update|
          @indexes.each do |index|
            index = index.to_id_graph if data[:by_id_graph]
            next unless update.modifies_index?(index)

            (0...@timesteps).each do |ts|
              min_cost.add @index_vars[index][ts] *
                               @data[:update_costs][update][index][ts]
            end
          end
        end
        min_cost
      end

      # add creation cost for new column family
      # @return [Array]
      def add_creation_cost(schema_cost)
        @indexes.each do |index|
          (1...@timesteps).each do |ts|
            cost = @include_migration_cost ? index.creation_cost(@creation_coeff) : @MIGRATE_COST_DUMMY_CONST
            schema_cost.add @migrate_vars[index][ts] * cost
          end
        end
        schema_cost
      end

      # calculate cf creation cost that exist from the start timestep
      def add_initial_index_loading_cost(schema_cost)
        @indexes.each do |index|
          cost = @include_migration_cost ? index.creation_cost(@creation_coeff) : @MIGRATE_COST_DUMMY_CONST
          schema_cost.add @index_vars[index][0] * cost
        end
        schema_cost
      end

      # add preparing cost for records of the new column family
      # @return [Array]
      def add_prepare_cost(schema_cost)
        cost = @queries.select{|q| q.is_a? MigrateSupportQuery}.reduce(MIPPeR::LinExpr.new) do |expr, query|
          expr.add(@indexes.reduce(MIPPeR::LinExpr.new) do |subexpr, index|
            next subexpr if @data[:costs][query][index].nil?
            subexpr.add((0...(@timesteps - 1)).reduce(MIPPeR::LinExpr.new) do |subsubexpr, ts|
              query_index_cost = @data[:costs][query][index]
              query_cost = @include_migration_cost ?
                               query_index_cost.last[ts] * 1.0
                               : @MIGRATE_COST_DUMMY_CONST
              cost_expr = @prepare_vars[index][query][ts] * query_cost
              subsubexpr.add cost_expr
            end)
          end)
        end

        schema_cost += cost
        schema_cost
      end

      # The total number of indexes
      # @return [MIPPeR::LinExpr]
      def total_indexes
        total = MIPPeR::LinExpr.new
        @index_vars.each_value { |vars| vars.each_value{|var| total += var * 1.0 }}

        total
      end

      # Return the selected indices
      # @return [Set<Index>]
      def selected_indexes
        return if @status.nil?
        return @selected_indexes if @selected_indexes

        @selected_indexes = []

        @selected_indexes = (0...@timesteps).map do |ts|
          @index_vars.each_key.select do |index|
            @index_vars[index][ts].value
          end.to_set
        end
        @selected_indexes
      end

      # Get the size of all indexes in the workload
      # @return [Array]
      def total_size_each_timestep
        normarlized_size_hash = calculate_index_normalized_sizes
        # TODO: Update for indexes grouped by ID path
        (0...@timesteps).map do |ts|
          @indexes.map do |index|
            # calculating total_size with 'normalized' size reduce the objective value and gives more precise result.
            @index_vars[index][ts] * normarlized_size_hash[index]
          end.reduce(&:+)
        end
      end

      def get_index_size_normalizer
        (10 ** (Math.log10(@indexes.map(&:size).max).to_i - 2)).to_f
      end

      # This method simply divides each index size by gcd.
      # General normalized that maps values to 0 to 1 produces, float value and increase computation costs
      def calculate_index_normalized_sizes
        normalizer = get_index_size_normalizer
        normalized_size_hash = {}
        @indexes.each do |index|
          normalized_size_hash[index] = index.size / normalizer
        end
        normalized_size_hash
      end

      # Get the size of all indexes in the workload
      # @return [MIPPeR::LinExpr]
      def total_size
        total_size_each_timestep.reduce(&:+)
      end

      def add_migration_variables
        add_cf_creation_variables
        add_cf_prepare_variables
        add_cf_prepare_tree_variables
      end

      # Initialize query and index variables
      # @return [void]
      def add_variables
        add_query_index_variable
        add_index_variable
        add_migration_variables
      end

      def add_query_index_variable
        @query_vars = {}
        @queries.each_with_index do |query, q|
          next if query.is_a? MigrateSupportQuery
          @data[:costs][query].keys.uniq.each do |related_index|
            @query_vars[related_index] = {} if @query_vars[related_index].nil?
            @query_vars[related_index][query] = {} if @query_vars[related_index][query].nil?
            (0...@timesteps).each do |ts|
              query_var = "q#{q}_#{related_index.key}_#{ts}" if ENV['NOSE_LOG'] == 'debug'
              var = MIPPeR::Variable.new 0, 1, 0, :binary, query_var
              @model << var
              @query_vars[related_index][query][ts] = var
            end
          end
        end
      end

      def add_index_variable
        @index_vars = {}
        @indexes.each do |index|
          @index_vars[index] = {}
          (0...@timesteps).each do |ts|
            var_name = "#{index.key}_#{ts}" if ENV['NOSE_LOG'] == 'debug'
            @index_vars[index][ts] = MIPPeR::Variable.new 0, 1, 0, :binary, var_name
          end

          # If needed when grouping by ID graph, add an extra
          # variable for the base index based on the ID graph
          next unless @data[:by_id_graph]
          id_graph = index.to_id_graph
          next if id_graph == index

          # Add a new variable for the ID graph if needed
          unless @index_vars.key? id_graph
            (0...@timesteps).each do |ts|
              var_name = "#{index.key}_#{ts}" if ENV['NOSE_LOG'] == 'debug'
              @index_vars[id_graph][ts] = MIPPeR::Variable.new 0, 1, 0, :binary,
                                                               var_name
            end
          end

          (0...@timesteps).each do |ts|
            # Ensure that the ID graph of this index is present if we use it
            name = "#{index.key}_#{ts}" if ENV['NOSE_LOG'] == 'debug'
            constr = MIPPeR::Constraint.new @index_vars[id_graph][ts] * 1.0 + \
                                          @index_vars[index][ts] * -1.0,
                                            :>=, 0, name
            @model << constr
          end
        end

        @index_vars.each_value { |vars| vars.each_value {|var| @model << var }}
      end

      # add variable for whether to create CF at the timestep
      # @return [void]
      def add_cf_creation_variables
        @migrate_vars = {}
        @indexes.each do |index|
          @migrate_vars[index] = {} if @migrate_vars[index].nil?
          # we do not need migrate_vars for timestep 0
          (1...@timesteps).each do |ts|
            name = "creation_var#{index.key}_#{ts - 1}_to_#{ts}" if ENV['NOSE_LOG'] == 'debug'
            var = MIPPeR::Variable.new 0, 1, 0, :binary, name
            @model << var
            @migrate_vars[index][ts] = var
          end
        end
      end

      # add variables for each preparing tree
      def add_cf_prepare_tree_variables
        @prepare_tree_vars = {}
        @migrate_prepare_plans.each do |index, query_trees|
          @prepare_tree_vars[index] = {} if @prepare_tree_vars[index].nil?
          query_trees.each do |query, _|
            @prepare_tree_vars[index][query] = {} if @prepare_tree_vars[index][query].nil?
            (0...(@timesteps - 1)).each do |ts|
              name = "prepare_tree_#{index.key}_#{ts}" if ENV['NOSE_LOG'] == 'debug'
              var = MIPPeR::Variable.new 0, 1, 0, :binary, name
              @model << var
              @prepare_tree_vars[index][query][ts] = var
            end
          end
        end
      end

      # add variable for whether to prepare data for the CF at the timestep
      # @return [void]
      def add_cf_prepare_variables
        @prepare_vars = {}
        @queries.select{|q| q.is_a? MigrateSupportQuery}.each do |migrate_support_query|
          @data[:costs][migrate_support_query].keys.uniq.each do |related_index|
            @prepare_vars[related_index] = {} if @prepare_vars[related_index].nil?
            @prepare_vars[related_index][migrate_support_query] = {} if @prepare_vars[related_index][migrate_support_query].nil?
            (0...(@timesteps - 1)).each do |ts|
              query_var = "mq_pr_#{related_index.key}_" +
                  "4_#{migrate_support_query.index.key}_#{ts}" if ENV['NOSE_LOG'] == 'debug'
              var = MIPPeR::Variable.new 0, 1, 0, :binary, query_var
              @model << var
              @prepare_vars[related_index][migrate_support_query][ts] = var
            end
          end
        end
      end

      # Prepare variables and constraints to account for the cost of sorting
      # @return [void]
      def prepare_sort_costs
        @sort_costs = {}
        @sort_vars = {}
        @data[:costs].each do |query, index_costs|
          @sort_costs[query] = {}
          @sort_vars[query] = {}

          index_costs.each do |index, (steps, _)|
            sort_step = steps.find { |s| s.is_a?(Plans::SortPlanStep) }
            next if sort_step.nil?

            (0...@timesteps).each do |ts|
              @sort_costs[query][index] = {} if @sort_costs[query][index].nil?
              @sort_vars[query][index] = {} if @sort_vars[query][index].nil?
              @sort_costs[query][index][ts] ||= sort_step.cost
              q = @queries.index query

              name = "sort_q#{q}_#{ts}" if ENV['NOSE_LOG'] == 'debug'
              sort_var = MIPPeR::Variable.new 0, 1, 0, :binary, name
              @sort_vars[query][index][ts] ||= sort_var
              @model << sort_var

              name = "q#{q}_#{index.key}_sort_#{ts}" if ENV['NOSE_LOG'] == 'debug'
              constr = MIPPeR::Constraint.new @sort_vars[query][index][ts] * 1.0 +
                                                  @query_vars[index][query][ts] * -1.0,
                                              :>=, 0, name
              @model << constr
            end
          end
        end
      end

      # Add all necessary constraints to the model
      # @return [void]
      def add_constraints
        constraints = [
          TimeDependIndexPresenceConstraints,
          TimeDependSpaceConstraint,
          TimeDependCompletePlanConstraints,
          TimeDependCreationConstraints,
          TimeDependPrepareConstraints,
          TimeDependPrepareTreeConstraints,
          TimeDependIndexesWithoutPreparePlanNotMigrated,
          TimeDependIndexCreatedAtUsedTimeStepConstraints,
        ]

        constraints.each do |constraint|
          constraint.apply self
        end

        @model.update
        @logger.debug do
          "Added #{@model.constraints.count} constraints to model"
        end
      end

      # Get the total cost of the query for the objective function
      # @return [MIPPeR::LinExpr]
      def total_query_cost(cost, query_var, sort_cost, sort_var, ts)
        return MIPPeR::LinExpr.new if cost.nil?
        query_cost = cost.last[ts] * 1.0

        cost_expr = query_var[ts] * query_cost
        cost_expr += sort_var[ts] * sort_cost[ts] unless sort_cost.nil?

        cost_expr
      end

      # Return relevant data on the results of the ILP
      # @return [Results]
      def result
        result = TimeDependResults.new self, @data[:by_id_graph]
        result.enumerated_indexes = indexes
        result.indexes = selected_indexes

        # TODO: Update for indexes grouped by ID path
        result.total_size = selected_indexes.map{|sindex_each_timestep| sindex_each_timestep.map(&:size).inject(&:+)}
        result.total_cost = @objective_value

        result
      end
    end
  end
end
