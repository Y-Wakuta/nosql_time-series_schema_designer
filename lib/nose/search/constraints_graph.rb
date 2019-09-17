# frozen_string_literal: true

module NoSE
  module Search
    # 各クエリにおいて、始点から１つのみ経路が作成されることを保証
    class OnePathConstraint < Constraint

      def self.start_one_path(problem_graph)
        problem_graph.edge_vars.each do |query, edge_var|
          start_edges = []
          edge_var.each do |from, edge|
            edge.each do |to, var|
              if from.is_a?(Plans::RootPlanStep)
                start_edges << var
              end
            end
          end
          start_paths = start_edges.map{|se| se * 1.0}.inject(:+)
          constr = MIPPeR::Constraint.new start_paths, :==, 1, "StartOnePathConstraint of #{query}"
          problem_graph.model << constr
        end
      end

      # guarantee only one path enter the last node for each query
      def self.last_one_path(problem_graph)
        problem_graph.edge_vars.each do |query, edge_var|
          edges_to_last = [] # クエリごとにある index について、その index を from にもつ edge が１つも無い場合に、そのノードに入っている edge をリストに追加。それらの変数の和が1になるようにする
          problem_graph.index_vars.map{|index, var| index}.each do |index|
            incoming_edges = []
            edge_var.each do |from, edge|
              edge.each do |to, var|
                if to.is_a?(Plans::IndexLookupPlanStep) and to.index == index
                  incoming_edges << var
                elsif from.is_a?(Plans::IndexLookupPlanStep) and from.index == index
                  incoming_edges = [] # if same edges out go from step that has the index, the step is not the last step
                end
              end
            end
            edges_to_last << incoming_edges unless incoming_edges.empty?
          end

          constr = MIPPeR::Constraint.new edges_to_last.flatten.map{|e| e * 1.0}.inject(:+), :==, 1, "LastOnePathConstraint of #{query}"
          problem_graph.model << constr
        end
      end

      def self.apply(problem_graph)
        start_one_path(problem_graph)
      #  last_one_path(problem_graph)
      end
    end

    # Guarantee that the number of nodes entering and leaving is the same
    class SameIOConstraint < Constraint

      def self.apply(problem_graph)
        problem_graph.edge_vars.each do |query, edge_var|
          problem_graph.get_indexes_by_query(query).each do |index|
            incoming_edges = MIPPeR::LinExpr.new
            outgoing_edges = MIPPeR::LinExpr.new
            edge_var.each do |from, edge|
              edge.each do |to, var|
                if to.is_a?(Plans::IndexLookupPlanStep) and to.index == index
                  incoming_edges += var * 1
                elsif from.is_a?(Plans::IndexLookupPlanStep) and from.index == index
                  outgoing_edges += var * (-1)
                end
              end
            end

            next if outgoing_edges.terms.size == 0 # if no edge go out from the index, the index is the last step of the plan

            constr = MIPPeR::Constraint.new incoming_edges + outgoing_edges , :==, 0, "io_same constraint for #{index.hash_str}"
            problem_graph.model << constr
          end
        end
      end
    end

    # guarantee that if one of incomming edge is used, the index is also used
    class PlanEdgeConstraints < Constraint
      def self.apply(problem_graph)
        edge_vars = problem_graph.edge_vars
        index_vars = problem_graph.index_vars

        whole_edge_num = edge_vars.size # the number which is larger than whole number of edges
        edge_vars.each do |query, edge_vars|
          target_index_vars = index_vars.select{|ind, _| problem_graph.get_indexes_by_query(query).include? ind}
          target_index_vars.each do |index, index_var|
            incoming_edge_vars = []
            edge_vars.each do |from, edge|
              edge.each do |to , var|
                next unless to.is_a? Plans::IndexLookupPlanStep
                incoming_edge_vars.append(var) if to.index == index
              end
            end

            incoming_edges_lin = incoming_edge_vars.map{|iev| iev * 1.0}.inject(:+)
            #constr = MIPPeR::Constraint.new incoming_edges_lin + (index_var * (-whole_edge_num)), :<=, 0, "plan_edge for #{index.hash_str}"
            constr = MIPPeR::Constraint.new incoming_edges_lin + (index_var * (-100)), :<=, 0, "plan_edge for #{index.hash_str}"
            problem_graph.model << constr
          end
        end
      end
    end
  end
end
