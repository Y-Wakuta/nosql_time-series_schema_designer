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
    class ProblemGraph < Problem
      attr_reader :trees, :adjacency_matrices, :edge_vars, :edge_costs

      def initialize(queries, trees, updates, data, objective = Objective::COST)

        @trees = trees
        set_adjacency_matrix

        super(queries, updates, data, objective)
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

      # Get the cost of all queries in the workload
      # @return [MIPPeR::LinExpr]
      def total_cost
        cost = MIPPeR::LinExpr.new
        @queries.each do |query|
          subexpr = MIPPeR::LinExpr.new
          @indexes.each do |index|
            @edge_vars[query].each do |from, edge|
              edge.each do |to, var|
                if to.index == index and not @edge_costs[query][from][to].nil? # TODO: decide by @edge_costs[query][from][to].nil? is dangerous because this based on only one path to 'to' exists. But this is not true.
                  subexpr += total_query_cost(@edge_costs[query][from][to],
                                               var,
                                               @sort_costs[query][index],
                                               @sort_vars[query][index])
                end
              end
            end
          end

          cost += subexpr
        end
        cost = add_update_costs cost
        cost
      end

      private

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
        super
        add_edge_variables
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
  end
end
