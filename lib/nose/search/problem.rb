require 'logging'

begin
  require 'guruby'
rescue LoadError
  # We can't use most search functionality, but it won't explode
  nil
end

module NoSE
  module Search
    # Simple enum for possible objective functions
    module Objective
      # Minimize the cost of statements in the workload
      COST  = 1

      # Minimize the space usage of generated indexes
      SPACE = 2

      # Minimize the total number of indexes
      INDEXES = 3
    end

    # A representation of a search problem as an ILP
    class Problem
      attr_reader :model, :status, :queries, :updates,
                  :index_vars, :query_vars, :indexes, :data,
                  :objective_type, :objective_value

      def initialize(queries, updates, indexes, data,
                     objective = Objective::COST)
        @queries = queries
        @updates = updates
        @indexes = indexes
        @data = data
        @logger = Logging.logger['nose::search::problem']
        @status = nil
        @objective_type = objective

        setup_model
      end

      # Run the solver and make the selected indexes available
      def solve
        return unless @status.nil?

        # Run the optimization
        @model.write '/tmp/search.lp'
        @model.optimize

        # Ensure we found a valid solution
        @status = model.status
        if @status == Guruby::GRB_INFEASIBLE
          fail
          # TODO Add IIS support to Guruby
          # @model.computeIIS
          # log_model 'IIS', '.ilp'

        # TODO Add dual objective support
        # elsif @objective_type != Objective::INDEXES
        #   @objective_value = @model.objective_value

        #   # Pin the objective value and optimize again to minimize index usage
        #   @obj_var.set_double Gurobi::DoubleAttr::UB, @objective_value
        #   @obj_var.set_double Gurobi::DoubleAttr::LB, @objective_value
        #   @objective_type = Objective::INDEXES
        #   define_objective 'objective_indexes'

        #   @status = nil
        #   solve
        #   return

        elsif @objective_value.nil?
          @objective_value = @model.objective_value
        end

        # TODO Add expression inspection to Guruby
        # @logger.debug do
        #   "Final objective value is #{@objective.active.inspect}" \
        #     " = #{@objective_value}"
        # end

        fail NoSolutionException if @status != Guruby::GRB_OPTIMAL
      end

      # Return the selected indices
      def selected_indexes
        return if @status.nil?
        return @selected_indexes if @selected_indexes

        @selected_indexes = @indexes.select do |index|
          @index_vars[index].value
        end.to_set
      end

      # Return relevant data on the results of the ILP
      def result
        result = Results.new self
        result.enumerated_indexes = indexes
        result.indexes = selected_indexes
        result.total_size = selected_indexes.map(&:size).inject(0, &:+)
        result.total_cost = @objective_value

        result
      end

      # Get the size of all indexes in the workload
      def total_size
        @indexes.map do |index|
          @index_vars[index] * (index.size * 1.0)
        end.reduce(&:+)
      end

      # Get the cost of all queries in the workload
      def total_cost
        cost = @queries.map do |query|
          @indexes.map do |index|
            total_query_cost @data[:costs][query][index],
              @query_vars[index][query]
          end.compact
        end.flatten.inject(&:+)

        cost = add_update_costs cost, data
        cost
      end

      # The total number of indexes
      def total_indexes
        total = Guruby::LinExpr.new
        @index_vars.each_value { |var| total += var * 1.0 }

        total
      end

      private

      # Write a model to a temporary file and log the file name
      def log_model(type, extension)
        @logger.debug do
          tmpfile = Tempfile.new ['model', extension]
          ObjectSpace.undefine_finalizer tmpfile
          @model.write tmpfile.path
          "#{type} written to #{tmpfile.path}"
        end
      end

      # Build the ILP by creating all the variables and constraints
      def setup_model
        # Set up solver environment
        env = Guruby::Environment.new

        # TODO Set environment parameters
        # env.set_int Gurobi::IntParam::METHOD,
        #             Gurobi::METHOD_DETERMINISTIC_CONCURRENT

        # # This copies the number of parallel processes used by the parallel gem
        # # so it implicitly respects the --no-parallel CLI flag
        # env.set_int Gurobi::IntParam::THREADS, Parallel.processor_count

        # # Disable console output since it's quite messy
        # env.set_int Gurobi::IntParam::OUTPUT_FLAG, 0

        @model = Guruby::Model.new env
        add_variables
        @model.update
        add_constraints
        define_objective
        @model.update

        log_model 'Model', '.lp'
      end

      private

      # Set the value of the objective function (workload cost)
      def define_objective(var_name = 'objective')
        obj = case @objective_type
              when Objective::COST
                total_cost
              when Objective::SPACE
                total_size
              when Objective::INDEXES
                total_indexes
              end

        # Add the objective function as a variable
        @obj_var = Guruby::Variable.new 0, Guruby::GRB_INFINITY, 1.0,
                                        Guruby::GRB_CONTINUOUS, var_name
        @model << @obj_var
        @model.update

        @model << Guruby::Constraint.new(obj + @obj_var * -1.0,
                                         Guruby::GRB_EQUAL, 0.0)

        # TODO Inspect objective function
        # @logger.debug { "Objective function is #{obj.inspect}" }

        @objective = obj
        @model.set_sense Guruby::GRB_MINIMIZE
      end

      # Initialize query and index variables
      def add_variables
        @index_vars = {}
        @query_vars = {}
        @indexes.each do |index|
          @index_vars[index] = Guruby::Variable.new 0, 1, 0,
                                                    Guruby::GRB_BINARY,
                                                    index.key
          @model << @index_vars[index]

          @query_vars[index] = {}
          @queries.each_with_index do |query, q|
            query_var = "q#{q}_#{index.key}"
            var = Guruby::Variable.new 0, 1, 0, Guruby::GRB_BINARY, query_var
            @model << var
            @query_vars[index][query] = var
          end
        end
      end

      # Add all necessary constraints to the model
      def add_constraints
        [
          IndexPresenceConstraints,
          SpaceConstraint,
          CompletePlanConstraints
        ].each { |constraint| constraint.apply self }

        # TODO Log constraints
        # @logger.debug do
        #   "Added #{@model.getConstrs.count} constraints to model"
        # end
      end

      # Deal with updates which do not require support queries
      def add_update_costs(min_cost, data)
        @updates.each do |update|
          @indexes.each do |index|
            next if !update.modifies_index?(index)

            min_cost += @index_vars[index] * data[:update_costs][update][index]
          end
        end

        min_cost
      end

      # Get the total cost of the query for the objective function
      def total_query_cost(cost, query_var)
        return if cost.nil?
        query_cost = cost.last * 1.0

        query_var * query_cost
      end
    end

    # Thrown when no solution can be found to the ILP
    class NoSolutionException < StandardError
    end
  end
end
