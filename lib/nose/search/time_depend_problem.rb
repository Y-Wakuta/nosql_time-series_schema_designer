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
      attr_reader :timesteps, :migrate_vars, :prepare_vars, :trees

      def initialize(queries, updates, data, objective = Objective::COST, timesteps)
        fail if timesteps.nil?

        @timesteps = timesteps
        @creation_cost = data[:creation_cost]
        @trees = data[:trees]
        super(queries, updates, data, objective)
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
        cost = @queries.reduce(MIPPeR::LinExpr.new) do |expr, query|
          expr.add(@indexes.reduce(MIPPeR::LinExpr.new) do |subexpr, index|
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
        cost = add_migration_cost cost
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
              min_cost.add @migrate_vars[index][ts + 1] * @data[:update_costs][update][index][ts]
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
            schema_cost.add @migrate_vars[index][ts] * index.creation_cost(@creation_cost)
          end
        end
        schema_cost
      end

      # add preparing cost for records of the new column family
      # @return [Array]
      def add_prepare_cost(schema_cost)
        @trees.each do |tree|
          tree.each do |plan|
            query_num = plan.steps.first.eq_filter.reduce(1){|_, field| field.parent.count}
            (1...@timesteps).each do |ts|
              schema_cost.add @prepare_vars[tree.query].find{|key, _| key == plan}.last[ts] * (plan.cost * query_num)
            end
          end
        end
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
        # TODO: Update for indexes grouped by ID path
        (0...@timesteps).map do |ts|
          @indexes.map do |index|
            @index_vars[index][ts] * (index.size * 1.0)
          end.reduce(&:+)
        end
      end

      # Get the size of all indexes in the workload
      # @return [MIPPeR::LinExpr]
      def total_size
        total_size_each_timestep.reduce(&:+)
      end

      # Initialize query and index variables
      # @return [void]
      def add_variables
        @index_vars = {}
        @query_vars = {}
        @indexes.each do |index|
          @query_vars[index] = {}
          @queries.each_with_index do |query, q|
            @query_vars[index][query] = {}
            (0...@timesteps).each do |ts|
              query_var = "q#{q}_#{index.key}_#{ts}" if ENV['NOSE_LOG'] == 'debug'
              var = MIPPeR::Variable.new 0, 1, 0, :binary, query_var
              @model << var
              @query_vars[index][query][ts] = var
            end
          end

          var_name = index.key if ENV['NOSE_LOG'] == 'debug'
          @index_vars[index] = {}
          (0...@timesteps).each do |ts|
            @index_vars[index][ts] = MIPPeR::Variable.new 0, 1, 0, :binary, var_name
          end

          # If needed when grouping by ID graph, add an extra
          # variable for the base index based on the ID graph
          next unless @data[:by_id_graph]
          id_graph = index.to_id_graph
          next if id_graph == index

          # Add a new variable for the ID graph if needed
          unless @index_vars.key? id_graph
            var_name = index.key if ENV['NOSE_LOG'] == 'debug'
            @index_vars[id_graph] = MIPPeR::Variable.new 0, 1, 0, :binary,
                                                         var_name
          end

          (0...@timesteps).each do |ts|
            # Ensure that the ID graph of this index is present if we use it
            name = "ID_#{id_graph.key}_#{index.key}_#{ts}" \
            if ENV['NOSE_LOG'] == 'debug'
            constr = MIPPeR::Constraint.new @index_vars[id_graph][ts] * 1.0 + \
                                          @index_vars[index][ts] * -1.0,
                                            :>=, 0, name
            @model << constr
          end
        end

        @index_vars.each_value { |vars| vars.each_value {|var| @model << var }}

        add_cf_creation_variables
        add_cf_prepare_variables
      end

      # add variable for whether to create CF at the timestep
      # @return [void]
      def add_cf_creation_variables
        @migrate_vars = {}
        @indexes.each do |index|
          @migrate_vars[index] = {} if @migrate_vars[index].nil?
          # we do not need migrate_vars for timestep 0
          (1...@timesteps).each do |ts|
            name = "s#{index.key}_#{ts - 1}_to_#{ts}" if ENV['NOSE_LOG'] == 'debug'
            var = MIPPeR::Variable.new 0, 1, 0, :binary, name
            @model << var
            @migrate_vars[index][ts] = var
          end
        end
      end

      # add variable for whether to prepare data for the CF at the timestep
      # @return [void]
      def add_cf_prepare_variables
        @prepare_vars = {}
        @trees.each do |tree|
          @prepare_vars[tree.query] = {} if @prepare_vars[tree.query].nil?
          tree.each do |plan|
            @prepare_vars[tree.query][plan] = {} if @prepare_vars[tree.query][plan].nil?
            (1..@timesteps).each do |ts|
              name = "p#{plan.inspect}_#{ts}" if ENV['NOSE_LOG'] == 'debug'
              var = MIPPeR::Variable.new 0, 1, 0, :binary, name
              @model << var
              @prepare_vars[tree.query][plan][ts] = var
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

              name = "s#{q}_#{ts}" if ENV['NOSE_LOG'] == 'debug'
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
        [
          TimeDependIndexPresenceConstraints,
          TimeDependSpaceConstraint,
          TimeDependCompletePlanConstraints,
          TimeDependCreationConstraints,
          TimeDependPrepareConstraints
        ].each { |constraint| constraint.apply self }

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
