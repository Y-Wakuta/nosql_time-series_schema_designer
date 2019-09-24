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
      attr_accessor :timesteps

      def initialize(queries, updates, data, objective = Objective::COST, timesteps)
        @timesteps = timesteps
        super(queries, updates, data, objective)
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
        cost
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
      # @return [MIPPeR::LinExpr]
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
      end

      # Add all necessary constraints to the model
      # @return [void]
      def add_constraints
        [
          TimeDependIndexPresenceConstraints,
          TimeDependSpaceConstraint,
          TimeDependCompletePlanConstraints
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
        cost_expr += sort_var * sort_cost unless sort_cost.nil?

        cost_expr
      end

      # Return relevant data on the results of the ILP
      # @return [Results]
      def result
        result = TimeDependResults.new self, @data[:by_id_graph]
        result.enumerated_indexes = indexes
        result.indexes = selected_indexes

        # TODO: Update for indexes grouped by ID path
        #result.total_size = selected_indexes.reduce(Set.new){|_, t| t}.map(&:size).inject(&:+)
        result.total_size = selected_indexes.map{|sindex_each_timestep| sindex_each_timestep.map(&:size).inject(&:+)}
        result.total_cost = @objective_value

        result
      end

    end

  end
end
