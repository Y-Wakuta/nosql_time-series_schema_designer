module NoSE
  module Cost
    RSpec.shared_examples 'dummy cost model' do
      let(:cost_model) do
        # Simple cost model which just counts the number of indexes
        class DummyCost < NoSE::Cost::Cost
          include Subtype

          def index_lookup_cost(_step)
            aggregates = (_step.index.count_fields + _step.index.sum_fields + _step.index.avg_fields + _step.index.groupby_fields).count() * 0.01

            return 1 + aggregates if _step.parent.is_a? Plans::RootPlanStep
            10 + aggregates
          end

          def insert_cost(_step)
            1
          end

          def delete_cost(_step)
            1
          end

          def prepare_insert_cost(_step)
            1
          end

          def prepare_delete_cost(_step)
            1
          end

          def extract_cost(step)
            1
          end

          def load_cost(index)
            1
          end
        end

        DummyCost.new
      end
    end
  end
end
