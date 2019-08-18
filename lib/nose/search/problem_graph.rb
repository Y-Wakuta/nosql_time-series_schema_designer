# frozen_string_literal: true

require 'logging'

require_relative './results_graph'
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
    class ProblemGraph
      attr_reader :model, :status, :queries, :updates,
                  :index_vars, :query_vars, :indexes, :data,
                  :objective_type, :objective_value, :trees, :adjacency_matrices, :edge_vars, :edge_costs

      def initialize(queries, trees, updates, data, objective = Objective::COST)
        @queries = queries
        @updates = updates
        @data = data
        @indexes = @data[:costs].flat_map { |_, ic| ic.keys }.uniq
        @logger = Logging.logger['nose::search::problem']
        @status = nil
        @objective_type = objective
        @trees = trees

        set_adjacency_matrix
        setup_model
      end

      ## 隣接行列に実際にノードを追加
      def add_node(query, node1, node2)
        if @adjacency_matrices[query].has_key?(node1) then
          @adjacency_matrices[query][node1].append(node2)
        else
          @adjacency_matrices[query][node1] = [node2]
        end
      end

      # ある接点とその子ノードについて、隣接行列に追加する
      def next_children(query, current)
        current.children.each do |child,_|
          add_node(query, current, child)
          #add_node(query, child, current)
          next_children(query, child)
        end
      end

      def set_adjacency_matrix
        @adjacency_matrices = {}
        @trees.each do |tree|
          @adjacency_matrices[tree.query] = {}
          next_children(tree.query, tree.root)
        end
      end

      # Run the solver and make the selected indexes available
      # @return [void]
      def solve(previous_type = nil)
        return unless @status.nil?

        # Run the optimization
        @model.optimize
        @status = model.status
        fail NoSolutionException, @status if @status != :optimized

        # Store the objective value
        @objective_value = @obj_var.value

        if @objective_type != Objective::INDEXES && previous_type.nil?
          solve_next Objective::INDEXES
          return
        elsif !previous_type.nil? && previous_type != Objective::SPACE
          solve_next Objective::SPACE
          return
        elsif @objective_value.nil?
          @objective_value = @model.objective_value
        end

        @logger.debug do
          "Final objective value is #{@objective.inspect}" \
            " = #{@objective_value}"
        end
      end

      # Return the selected indices
      # @return [Set<Index>]
      def selected_indexes
        return if @status.nil?
        return @selected_indexes if @selected_indexes

        @selected_indexes = @index_vars.each_key.select do |index|
          @index_vars[index].value
        end.to_set
      end

      # Return relevant data on the results of the ILP
      # @return [Results]
      def result
        result = ResultsGraph.new self, @data[:by_id_graph]
        result.enumerated_indexes = indexes
        result.indexes = selected_indexes

        # TODO: Update for indexes grouped by ID path
        result.total_size = selected_indexes.sum_by(&:size)
        result.total_cost = @objective_value

        result
      end

      # Get the size of all indexes in the workload
      # @return [MIPPeR::LinExpr]
      def total_size
        # TODO: Update for indexes grouped by ID path
        @indexes.map do |index|
          @index_vars[index] * (index.size * 1.0)
        end.reduce(&:+)
      end

      # Get the cost of all queries in the workload
      # @return [MIPPeR::LinExpr]
      def total_cost
        cost = @queries.reduce(MIPPeR::LinExpr.new) do |expr, query|
          expr.add(@indexes.reduce(MIPPeR::LinExpr.new) do |subexpr, index|
            subexpr.add total_query_cost(@data[:costs][query][index],
                                         @query_vars[index][query],
                                         @sort_costs[query][index],
                                         @sort_vars[query][index])
          end)
        end

        cost = add_update_costs cost
        cost
      end

      # The total number of indexes
      # @return [MIPPeR::LinExpr]
      def total_indexes
        total = MIPPeR::LinExpr.new
        @index_vars.each_value { |var| total += var * 1.0 }

        total
      end

      private

      # Pin the current objective value and set a new objective
      # @return [void]
      def solve_next(objective_type)
        @obj_var.lower_bound = @objective_value
        @obj_var.upper_bound = @objective_value

        if objective_type == Objective::INDEXES
          @objective_type = Objective::INDEXES
          define_objective 'objective_indexes'
        elsif objective_type == Objective::SPACE
          @objective_type = Objective::SPACE
          define_objective 'objective_space'
        end

        @status = nil
        solve objective_type
      end

      # Write a model to a temporary file and log the file name
      # @return [void]
      def log_model(type)
        @logger.debug do
          tmpfile = Tempfile.new ['model', '.mps']
          ObjectSpace.undefine_finalizer tmpfile
          @model.write_mps tmpfile.path
          "#{type} written to #{tmpfile.path}"
        end
      end

      # Build the ILP by creating all the variables and constraints
      # @return [void]
      def setup_model
        # Set up solver environment
        @model = MIPPeR::CbcModel.new

        add_variables
        calculate_edge_cost
        prepare_sort_costs
        @model.update

        add_constraints
        define_objective
        @model.update

        log_model 'Model'
      end

      private

      # Set the value of the objective function (workload cost)
      # @return [void]
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
        var_name = nil unless ENV['NOSE_LOG'] == 'debug'
        @obj_var = MIPPeR::Variable.new 0, Float::INFINITY, 1.0,
                                        :continuous, var_name
        @model << @obj_var
        @model.update

        @model << MIPPeR::Constraint.new(obj + @obj_var * -1.0, :==, 0.0)

        @logger.debug { "Objective function is #{obj.inspect}" }

        @objective = obj
        @model.sense = :min
      end

      # make edge_vars from adjacency matrix
      def add_edge_variables
        @edge_vars = {}
        @adjacency_matrices.each do |query, adjacency_matrix|
          @edge_vars[query] = {}
          adjacency_matrix.each do |from, nodes|
            @edge_vars[query][from] = {}
            nodes.each do |to|
              edge_name = from.to_s + " -> " + to.to_s
              var = MIPPeR::Variable.new 0, 1, 0, :binary, edge_name
              @model << var
              @edge_vars[query][from][to] = var
            end
          end
        end
      end

      # Initialize query and index variables
      # @return [void]
      def add_variables
        @index_vars = {}
        @query_vars = {}
        @indexes.each do |index|
          @query_vars[index] = {}
          @queries.each_with_index do |query, q|
            query_var = "q#{q}_#{index.key}" if ENV['NOSE_LOG'] == 'debug'
            var = MIPPeR::Variable.new 0, 1, 0, :binary, query_var
            @model << var
            @query_vars[index][query] = var
          end

          var_name = index.key if ENV['NOSE_LOG'] == 'debug'
          @index_vars[index] = MIPPeR::Variable.new 0, 1, 0, :binary, var_name

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

          # Ensure that the ID graph of this index is present if we use it
          name = "ID_#{id_graph.key}_#{index.key}" \
            if ENV['NOSE_LOG'] == 'debug'
          constr = MIPPeR::Constraint.new @index_vars[id_graph] * 1.0 + \
                                          @index_vars[index] * -1.0,
                                          :>=, 0, name
          @model << constr
        end

        @index_vars.each_value { |var| @model << var }

        add_edge_variables
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

            @sort_costs[query][index] ||= sort_step.cost
            q = @queries.index query

            name = "s#{q}" if ENV['NOSE_LOG'] == 'debug'
            sort_var = MIPPeR::Variable.new 0, 1, 0, :binary, name
            @sort_vars[query][index] ||= sort_var
            @model << sort_var

            name = "q#{q}_#{index.key}_sort" if ENV['NOSE_LOG'] == 'debug'
            constr = MIPPeR::Constraint.new @sort_vars[query][index] * 1.0 +
                                            @query_vars[index][query] * -1.0,
                                            :>=, 0, name
            @model << constr
          end
        end
      end

      # Add all necessary constraints to the model
      # @return [void]
      def add_constraints
        [
          SpaceConstraint,
          SameIOConstraint,
          OnePathConstraint,
          PlanEdgeConstraints
        ].each { |constraint| constraint.apply self }

        @logger.debug do
          "Added #{@model.constraints.count} constraints to model"
        end
      end

      # Deal with updates which do not require support queries
      # @return [MIPPeR::LinExpr]
      def add_update_costs(min_cost)
        @updates.each do |update|
          @indexes.each do |index|
            index = index.to_id_graph if data[:by_id_graph]
            next unless update.modifies_index?(index)

            min_cost.add @index_vars[index] *
                         @data[:update_costs][update][index]
          end
        end

        min_cost
      end

      # Get the total cost of the query for the objective function
      # @return [MIPPeR::LinExpr]
      def total_query_cost(cost, query_var, sort_cost, sort_var)
        return MIPPeR::LinExpr.new if cost.nil?
        query_cost = cost.last * 1.0

        cost_expr = query_var * query_cost
        cost_expr += sort_var * sort_cost unless sort_cost.nil?

        cost_expr
      end

      # convert C_ij to C_e (temporary way)
      def calculate_edge_cost()
        @edge_costs = {}
        @edge_vars.each do |query, edge_var|
          @edge_costs[query] = {}
          @data[:costs][query].each do |index, cost|
            edge_var.each do |from, edge|
              edge.each do |to, var|
                next unless to.is_a? Plans::IndexLookupPlanStep

                # suppose that there is no dag edge.
                # TODO: use 'from' tag to specify cost for the target edge. this is difficult by using C_ij, so we can directly calculate C_e at cost estimation step
                # TODO: in paper of NoSE, Fig 6 allows dag query plan in CF5. ask prof. mior is this ok or not.
                if to.index == index
                  @edge_costs[query][from] = {}
                  @edge_costs[query][from][to] = cost
                end
              end
            end
          end
        end
      end
    end

    # Thrown when no solution can be found to the ILP
    class NoSolutionException < StandardError
    end
  end
end
