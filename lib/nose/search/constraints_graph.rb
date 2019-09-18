# frozen_string_literal: true

module NoSE
  module Search

    # Guarantee that only one path starts from the root node in each query
    class OnePathConstraint < Constraint
      def self.apply(problem_graph)
        problem_graph.edge_vars.each do |query, edge_var|
          start_paths = problem_graph.enumerate_edge(edge_var)
                          .select{|from, _, _| from.is_a? Plans::RootPlanStep}
                          .map{|_, _, var| var * 1.0}
                          .inject(:+)
          constr = MIPPeR::Constraint.new start_paths, :==, 1, "StartOnePathConstraint of #{query.text}"
          problem_graph.model << constr
        end
      end
    end

    # Guarantee that the number of nodes entering and leaving is the same
    class SameIOConstraint < Constraint
      def self.apply(problem_graph)
        problem_graph.edge_vars.each do |query, edge_var|
          problem_graph.get_indexes_by_query(query).each do |index|
            incoming_edges = MIPPeR::LinExpr.new
            outgoing_edges = MIPPeR::LinExpr.new
            problem_graph.enumerate_edge(edge_var).each do |from, to, var|
              if to.is_a?(Plans::IndexLookupPlanStep) and to.index == index
                incoming_edges += var * 1
              elsif from.is_a?(Plans::IndexLookupPlanStep) and from.index == index
                outgoing_edges += var * (-1)
              end
            end

            next if outgoing_edges.terms.size == 0 # if no edge go out from the index, the index is the last step of the plan

            constr = MIPPeR::Constraint.new incoming_edges + outgoing_edges , :==, 0, "io_same constraint for #{index.hash_str}"
            problem_graph.model << constr
          end

          # iterate for FilterPlanStep or SortPlanStep
          problem_graph.enumerate_edge(edge_var).each do |_, to, _|
            next if not (to.is_a? Plans::FilterPlanStep or to.is_a? Plans::SortPlanStep)

            # collect vars of incoming edges
            incomes = problem_graph.enumerate_edge(edge_var)
              .select{|_, income_to, _| to == income_to}
              .map{|_, _, var| var * 1.0}
              .inject(:+)

            # collect vars of outgoing edges
            outgos = problem_graph.enumerate_edge(edge_var)
                        .select{|outgo_from, _, _| to == outgo_from}
                        .map{|_, _, var| var * (-1.0)}
                        .inject(:+)

            constr = MIPPeR::Constraint.new incomes + outgos , :==, 0, "io_same constraint for non-IndexLookupStep"
            problem_graph.model << constr
          end
        end
      end
    end

    # guarantee that if one of incomming edge is used, the index is also used
    class PlanEdgeConstraints < Constraint
      def self.apply(problem_graph)

        # the number which is larger than whole number of edges
        whole_edge_count = problem_graph
                             .edge_vars
                             .map{|_, edge_var| edge_var.map{|_, edge| edge.map{|_, var| var}}}
                             .flatten.size

        problem_graph.edge_vars.each do |query, edge_var|
          target_index_vars = problem_graph
                                .index_vars
                                .select{|ind, _| problem_graph.get_indexes_by_query(query).include? ind}
          target_index_vars.each do |index, index_var|
            incoming_edge_vars = []
            problem_graph.enumerate_edge(edge_var).each do |_, to, var|
              next unless to.is_a? Plans::IndexLookupPlanStep
              incoming_edge_vars << var if to.index == index
            end

            incoming_edges_lin = incoming_edge_vars
                                   .map{|iev| iev * 1.0}
                                   .inject(:+)
            constr = MIPPeR::Constraint.new incoming_edges_lin + index_var * (-whole_edge_count), :<=, 0, "plan_edge for #{index.hash_str}. #{incoming_edge_vars.size} incoming edges"
            problem_graph.model << constr
          end
        end
      end
    end
  end
end
