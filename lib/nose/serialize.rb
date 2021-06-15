# frozen_string_literal: true

require 'json-schema'
require 'representable'
require 'representable/json'
require 'representable/yaml'

# XXX Caching currently breaks the use of multiple formatting modules
#     see https://github.com/apotonick/representable/issues/180
module Representable
  # Break caching used by representable to allow multiple representers
  module Uncached
    # Create a simple binding which does not use caching
    def representable_map(options, format)
      Representable::Binding::Map.new(
        representable_bindings_for(format, options)
      )
    end
  end
end

module NoSE
  # Serialization of workloads and statement execution plans
  module Serialize
    # Validate a string of JSON based on the schema
    def validate_json(json)
      schema_file = File.join File.dirname(__FILE__), '..', '..',
                              'data', 'nose', 'nose-schema.json'
      schema = JSON.parse File.read(schema_file)

      data = JSON.parse json
      JSON::Validator.validate(schema, data)
    end
    module_function :validate_json

    # Construct a field from a parsed hash
    class FieldBuilder
      include Uber::Callable

      def call(_, fragment:, user_options:, **)
        field_class = Fields::Field.subtype_class fragment['type']

        # Extract the correct parameters and create a new field instance
        if field_class == Fields::StringField && !fragment['size'].nil?
          field = field_class.new fragment['name'], fragment['size']
        elsif field_class.ancestors.include? Fields::ForeignKeyField
          entity = user_options[:entity_map][fragment['entity']]
          field = field_class.new fragment['name'], entity, composite: fragment['composite_keys']
        elsif field_class == Fields::IDField
          field = field_class.new fragment['name'], composite: fragment['composite_keys']
        else
          field = field_class.new fragment['name']
        end

        field *= fragment['cardinality'] if fragment['cardinality']

        field
      end
    end

    # Represents a field just by the entity and name
    class FieldRepresenter < Representable::Decorator
      include Representable::JSON
      include Representable::YAML
      include Representable::Hash
      include Representable::Uncached

      property :name

      # The name of the parent entity
      def parent
        represented.parent.name
      end
      property :parent, exec_context: :decorator
    end

    # Represents a graph by its nodes and edges
    class GraphRepresenter < Representable::Decorator
      include Representable::JSON
      include Representable::YAML
      include Representable::Hash
      include Representable::Uncached

      def nodes
        represented.nodes.map { |n| n.entity.name }
      end

      property :nodes, exec_context: :decorator

      def edges
        represented.unique_edges.map do |edge|
          FieldRepresenter.represent(edge.key).to_hash
        end
      end

      property :edges, exec_context: :decorator
    end

    # Reconstruct indexes with fields from an existing workload
    class IndexBuilder
      include Uber::Callable

      def call(_, represented:, fragment:, **)
        # Extract the entities from the workload
        model = represented.model

        # Pull the fields from each entity
        f = lambda do |fields|
          fields.map { |dict|
            model[dict['parent']].foreign_keys[dict['name']] || model[dict['parent']][dict['name']]
          }
        end

        graph_entities = fragment['graph']['nodes'].map { |n| model[n] }
        graph_keys = f.call(fragment['graph']['edges'])
        graph = QueryGraph::Graph.new graph_entities
        graph_keys.each { |k| graph.add_edge k.parent, k.entity, k }

        Index.new f.call(fragment['hash_fields']),
                  f.call(fragment['order_fields']),
                  f.call(fragment['extra']),
                  graph,
                  count_fields: fragment.has_key?('count_fields') ? f.call(fragment['count_fields']).to_set : Set.new,
                  sum_fields: fragment.has_key?('sum_fields') ? f.call(fragment['sum_fields']).to_set : Set.new,
                  max_fields: fragment.has_key?('max_fields') ? f.call(fragment['max_fields']).to_set : Set.new,
                  avg_fields: fragment.has_key?('avg_fields') ? f.call(fragment['avg_fields']).to_set : Set.new,
                  groupby_fields: fragment.has_key?('groupby_fields') ? f.call(fragment['groupby_fields']).to_set : Set.new,
                  saved_key: fragment['key']
      end
    end

    # Represents a simple key for an index
    class IndexRepresenter < Representable::Decorator
      include Representable::JSON
      include Representable::YAML
      include Representable::Hash
      include Representable::Uncached

      property :key
    end

    class EachTimestepIndexesBuilder
      include Uber::Callable

      def call(_, fragment:, represented:, **)
        indexes = fragment['indexes'].map do |index|
          IndexBuilder.new.call(_, fragment: index, represented: represented)
        end
        EachTimeStepIndexes.new(indexes)
      end
    end

    # Represents index data along with the key
    class FullIndexRepresenter < IndexRepresenter
      collection :hash_fields, decorator: FieldRepresenter
      collection :order_fields, decorator: FieldRepresenter
      collection :extra, decorator: FieldRepresenter

      property :graph, decorator: GraphRepresenter
      property :entries
      property :entry_size
      property :size
      property :hash_count
      property :per_hash_count
      collection :count_fields, decorator: FieldRepresenter
      collection :sum_fields, decorator: FieldRepresenter
      collection :max_fields, decorator: FieldRepresenter
      collection :avg_fields, decorator: FieldRepresenter
      collection :groupby_fields, decorator: FieldRepresenter
    end

    class TimeDependIndexesBuilder
      include Uber::Callable

      def call(_, fragment:, represented:, **)
        indexes_all_timestep = fragment['indexes_all_timestep'].map do |iat|
          EachTimestepIndexesBuilder.new.call(_, fragment: iat, represented: represented)
        end
        TimeDependIndexes.new(indexes_all_timestep)
      end
    end

    class EachTimeIndexesRepsenter < Representable::Decorator
      include Representable::JSON
      include Representable::YAML
      include Representable::Hash
      include Representable::Uncached

      collection :indexes, class: Object, decorator: FullIndexRepresenter, deserialize: IndexBuilder.new
    end

    class TimeDependIndexesRepsenter < Representable::Decorator
      include Representable::JSON
      include Representable::YAML
      include Representable::Hash
      include Representable::Uncached

      collection :indexes_all_timestep, class: Object, decorator: EachTimeIndexesRepsenter, deserialize: EachTimestepIndexesBuilder.new
    end

    # Represents all data of a field
    class EntityFieldRepresenter < Representable::Decorator
      include Representable::JSON
      include Representable::YAML
      include Representable::Hash
      include Representable::Uncached

      collection_representer class: Object, deserialize: FieldBuilder.new

      property :name
      property :size
      property :cardinality
      property :subtype_name, as: :type

      # The entity name for foreign keys
      # @return [String]
      def entity
        represented.entity.name \
          if represented.is_a? Fields::ForeignKeyField
      end
      property :entity, exec_context: :decorator

      # The cardinality of the relationship
      # @return [Symbol]
      def relationship
        represented.relationship \
          if represented.is_a? Fields::ForeignKeyField
      end
      property :relationship, exec_context: :decorator

      # The reverse
      # @return [String]
      def reverse
        represented.reverse.name \
          if represented.is_a? Fields::ForeignKeyField
      end
      property :reverse, exec_context: :decorator

      def composite_keys
        represented.composite_keys \
          if represented.instance_of?(Fields::IDField) or represented.instance_of?(Fields::ForeignKeyField)
      end
      collection :composite_keys, exec_context: :decorator, deserialize: FieldBuilder
    end

    # Reconstruct the fields of an entity
    class EntityBuilder
      include Uber::Callable

      def call(_, fragment:, user_options:, **)
        # Pull the field from the map of all entities
        entity_map = user_options[:entity_map]
        entity = entity_map[fragment['name']]

        # Add all fields from the entity
        fields = EntityFieldRepresenter.represent([])
        fields = fields.from_hash fragment['fields'],
                                  user_options: { entity_map: entity_map }
        fields.each { |field| entity.send(:<<, field, freeze: false) }

        entity
      end
    end

    # Represent the whole entity and its fields
    class EntityRepresenter < Representable::Decorator
      include Representable::JSON
      include Representable::YAML
      include Representable::Hash
      include Representable::Uncached

      collection_representer class: Object, deserialize: EntityBuilder.new

      property :name
      collection :fields, decorator: EntityFieldRepresenter,
                 exec_context: :decorator
      property :count

      # A simple array of the fields within the entity
      def fields
        represented.fields.values + represented.foreign_keys.values
      end
    end

    # Conversion of a statement is just the text
    class StatementRepresenter < Representable::Decorator
      include Representable::JSON
      include Representable::YAML
      include Representable::Hash
      include Representable::Uncached

      # Represent as the text of the statement
      def to_hash(*)
        represented.text
      end
    end

    # Base representation for query plan steps
    class PlanStepRepresenter < Representable::Decorator
      include Representable::JSON
      include Representable::YAML
      include Representable::Hash
      include Representable::Uncached

      property :subtype_name, as: :type
      property :cost

      # The estimated cardinality at this step in the plan
      def cardinality
        state = represented.instance_variable_get(:@state)
        state.cardinality unless state.nil?
      end
      property :cardinality, exec_context: :decorator

      # The estimated hash cardinality at this step in the plan
      # @return [Fixnum]
      def hash_cardinality
        state = represented.instance_variable_get(:@state)
        state.hash_cardinality if state.is_a?(Plans::QueryState)
      end
      property :hash_cardinality, exec_context: :decorator
    end

    # Represent the index for index lookup plan steps
    class IndexLookupStepRepresenter < PlanStepRepresenter
      property :index, decorator: IndexRepresenter
      collection :eq_filter, decorator: FieldRepresenter
      property :range_filter, decorator: FieldRepresenter
      collection :order_by, decorator: FieldRepresenter
      property :limit
    end

    class AggregationStepRepresenter < PlanStepRepresenter
      collection :counts, decorator: FieldRepresenter
      collection :sums, decorator: FieldRepresenter
      collection :avgs, decorator: FieldRepresenter
      collection :maxes, decorator: FieldRepresenter
      collection :groupby, decorator: FieldRepresenter
    end

    # Represent the filtered fields in filter plan steps
    class FilterStepRepresenter < PlanStepRepresenter
      collection :eq, decorator: FieldRepresenter
      property :range, decorator: FieldRepresenter
    end

    # Represent the sorted fields in filter plan steps
    class SortStepRepresenter < PlanStepRepresenter
      collection :sort_fields, decorator: FieldRepresenter
    end

    # Represent the limit for limit plan steps
    class LimitStepRepresenter < PlanStepRepresenter
      property :limit
    end

    # Represent a query plan as a sequence of steps
    class QueryPlanRepresenter < Representable::Decorator
      include Representable::JSON
      include Representable::YAML
      include Representable::Hash
      include Representable::Uncached

      property :group
      property :name
      property :query, decorator: StatementRepresenter
      property :cost
      property :weight
      collection :each, as: :steps, decorator: (lambda do |options|
        {
          index_lookup: IndexLookupStepRepresenter,
          filter: FilterStepRepresenter,
          sort: SortStepRepresenter,
          limit: LimitStepRepresenter,
          aggregation: AggregationStepRepresenter
        }[options[:input].class.subtype_name.to_sym] || PlanStepRepresenter
      end)
    end

    class TimeDependPlanRepresenter < Representable::Decorator
      include Representable::JSON
      include Representable::YAML
      include Representable::Hash
      include Representable::Uncached

      property :query, decorator: StatementRepresenter
      collection :plans, decorator: QueryPlanRepresenter, class: Object
    end

    # Represent update plan steps
    class UpdatePlanStepRepresenter < PlanStepRepresenter
      property :index, decorator: IndexRepresenter
      collection :fields, decorator: FieldRepresenter

      # Set the hidden type variable
      # @return [Symbol]
      def type
        represented.instance_variable_get(:@type)
      end

      # Set the hidden type variable
      # @return [void]
      def type=(type)
        represented.instance_variable_set(:@type, type)
      end

      property :type, exec_context: :decorator

      # The estimated cardinality of entities being updated
      # @return [Fixnum]
      def cardinality
        state = represented.instance_variable_get(:@state)
        state.cardinality unless state.nil?
      end

      property :cardinality, exec_context: :decorator
    end

    # Represent an update plan
    class UpdatePlanRepresenter < Representable::Decorator
      include Representable::JSON
      include Representable::YAML
      include Representable::Hash
      include Representable::Uncached

      property :group
      property :name
      property :cost
      property :update_cost
      property :weight
      property :statement, decorator: StatementRepresenter
      property :index, decorator: IndexRepresenter

      collection :query_plans, class: Object, decorator: QueryPlanRepresenter

      collection :update_steps, decorator: UpdatePlanStepRepresenter

      # The backend cost model used to cost the updates
      # @return [Cost::Cost]
      def cost_model
        options = represented.cost_model.instance_variable_get(:@options)
        options[:name] = represented.cost_model.subtype_name
        options
      end

      # Look up the cost model by name and attach to the results
      # @return [void]
      def cost_model=(options)
        options = options.deep_symbolize_keys
        cost_model_class = Cost::Cost.subtype_class(options[:name])
        represented.cost_model = cost_model_class.new(**options)
      end

      property :cost_model, exec_context: :decorator
    end

    # Reconstruct the steps of an update plan
    class UpdatePlanBuilder
      include Uber::Callable

      def call(_, fragment:, represented:, **)
        workload = represented.workload

        if fragment['statement'].nil?
          statement = OpenStruct.new group: fragment['group']
        else
          statement = Statement.parse fragment['statement'], workload.model,
                                      group: fragment['group']
        end

        update_steps = fragment['update_steps'].map do |step_hash|
          step_class = Plans::PlanStep.subtype_class step_hash['type']
          index_key = step_hash['index']['key']
          step_index = represented.indexes.find { |i| i.key == index_key }

          if statement.nil?
            state = nil
          else
            state = Plans::UpdateState.new statement, step_hash['cardinality']
          end
          step = step_class.new step_index, state

          # Set the fields to be inserted
          fields = (step_hash['fields'] || []).map do |dict|
            workload.model[dict['parent']][dict['name']]
          end
          step.instance_variable_set(:@fields, fields) \
            if step.is_a?(Plans::InsertPlanStep)

          step
        end

        index_key = fragment['index']['key']
        index = represented.indexes.find { |i| i.key == index_key }
        update_plan = Plans::UpdatePlan.new statement, index, [], update_steps,
                                            represented.cost_model

        update_plan.instance_variable_set(:@group, fragment['group']) \
          unless fragment['group'].nil?
        update_plan.instance_variable_set(:@name, fragment['name']) \
          unless fragment['name'].nil?
        update_plan.instance_variable_set(:@weight, fragment['weight'])

        # Reconstruct and assign the query plans
        builder = QueryPlanBuilder.new
        query_plans = fragment['query_plans'].map do |plan|
          builder.call [], represented: represented, fragment: plan
        end
        update_plan.instance_variable_set(:@query_plans, query_plans)
        update_plan.send :update_support_fields

        update_plan
      end
    end

    class TimeDependUpdatePlanEachTimestepRepresenter < Representable::Decorator
      include Representable::JSON
      include Representable::YAML
      include Representable::Hash
      include Representable::Uncached

      collection :plans, class: Object, decorator: UpdatePlanRepresenter, deserialize: UpdatePlanBuilder.new
    end


    class TimeDependUpdatePlanRepresenter < Representable::Decorator
      include Representable::JSON
      include Representable::YAML
      include Representable::Hash
      include Representable::Uncached

      property :statement, class: Object, decorator: StatementRepresenter
      collection :plans_all_timestep, class: Object, decorator: TimeDependUpdatePlanEachTimestepRepresenter
    end

    class TimeDependUpdatePlanBuilder
      include Uber::Callable

      def call(_, fragment:, represented:, **)
        update_plans_hashs = fragment['plans_all_timestep'].map{|plan_each_timestep| plan_each_timestep.values}.flatten(1)
        represented.indexes = represented.time_depend_indexes.indexes_all_timestep.map do |each_time_step_indexes|
          each_time_step_indexes.indexes
        end.flatten(1).uniq!

        plans_all_timestep = update_plans_hashs.map do |update_plans_hash|
          update_plans_hash.map do |uph|
            UpdatePlanBuilder.new.call(_, fragment: uph, represented: represented)
          end
        end

        Plans::TimeDependUpdatePlan.new(fragment['statement'],plans_all_timestep)
      end
    end

    class MigratePreparePlanBuilder
      include Uber::Callable

      def call(_, fragment:, represented:, **)
        index = IndexBuilder.new.call(_, fragment: fragment['index'], represented: represented)
        query_plan = QueryPlanBuilder.new.call(_, fragment: fragment['query_plan'], represented: represented)
        Plans::MigratePreparePlan.new(index, query_plan, fragment['timestep'])
      end
    end

    class MigratePlanBuilder
      include Uber::Callable

      def call(_, fragment:, represented:, **)
        represented.indexes = represented.time_depend_indexes.indexes_all_timestep.map do |each_time_step_indexes|
          each_time_step_indexes.indexes
        end.flatten(1).uniq!
        obsolete_plan = QueryPlanBuilder.new.call(_, fragment: fragment['obsolete_plan'], represented: represented)
        new_plan = QueryPlanBuilder.new.call(_, fragment: fragment['new_plan'], represented: represented)
        migrate_plan = Plans::MigratePlan.new(fragment['query'], fragment['start_time'], obsolete_plan, new_plan)
        migrate_plan.prepare_plans = fragment['prepare_plans'].map do |prepare_plan|
          MigratePreparePlanBuilder.new.call(_, fragment: prepare_plan, represented: represented)
        end
        migrate_plan
      end
    end

    class TimeDependPlanBuilder
      include Uber::Callable

      def call(_, fragment:, represented:, **)
        represented.indexes = represented.time_depend_indexes.indexes_all_timestep.map do |each_time_index|
          each_time_index.indexes
        end.flatten(1).to_set
        plans = fragment['plans'].map do |plan_hash|
          QueryPlanBuilder.new.call(_, fragment: plan_hash, represented: represented)
        end

        Plans::TimeDependPlan.new(fragment['query'], plans)
      end
    end

    class BasicWorkloadRepresenter < Representable::Decorator
      include Representable::JSON
      include Representable::YAML
      include Representable::Hash
      include Representable::Uncached

      collection :statements, decorator: StatementRepresenter
      property :mix

      def workload_weights
        represented.instance_variable_get(:@statement_weights)
      end

      # Produce weights of each statement in the workload for each mix
      # @return [Hash]
      def weights
        weights = {}
        workload_weights.each do |mix, mix_weights|
          weights[mix] = {}
          mix_weights.each do |statement, weight|
            statement = StatementRepresenter.represent(statement).to_hash
            weights[mix][statement] = weight
          end
        end

        weights
      end
    end

    # Represent statements in a workload
    class WorkloadRepresenter < BasicWorkloadRepresenter
      property :weights, exec_context: :decorator
    end

    class TimeDependWorkloadRepresenter < BasicWorkloadRepresenter
      collection :weights, exec_context: :decorator
      property :is_static
      property :timesteps
      property :interval

      def workload_weights
        represented.instance_variable_get(:@time_depend_statement_weights)
      end
    end

    # Represent entities in a model
    class ModelRepresenter < Representable::Decorator
      include Representable::JSON
      include Representable::YAML
      include Representable::Hash
      include Representable::Uncached

      # A simple array of the entities in the model
      # @return [Array<Entity>]
      def entities
        represented.entities.values
      end
      collection :entities, decorator: EntityRepresenter,
                 exec_context: :decorator
    end

    # Construct a new workload from a parsed hash
    class WorkloadBuilder
      include Uber::Callable

      def call(_, input:, fragment:, represented:, **)
        workload = input.represented
        workload.instance_variable_set :@model, represented.model
        workload.timesteps = fragment['timesteps']
        workload.interval = fragment['interval']
        workload.is_static = fragment['is_static'] if workload.is_a? TimeDependWorkload

        # Add all statements to the workload
        statement_weights = Hash.new { |h, k| h[k] = {} }
        fragment['weights'].each do |mix, weights|
          mix = mix.to_sym
          weights.each do |statement, weight|
            # weight is multiplied by the interval and multiplied by interval again in add_statement method.
            # therefore, devide weight by the interval twice
            statement_weights[statement][mix] = weight.map{|w| w.to_f / (workload.interval * workload.interval)}
          end
        end
        fragment['statements'].each do |statement|
          workload.add_statement statement, statement_weights[statement],
                                 group: fragment['group']
        end

        workload.mix = fragment['mix'].to_sym unless fragment['mix'].nil?

        workload
      end
    end

    class ModelBuilder
      include Uber::Callable

      def call(_, input:, fragment:, **)
        model = input.represented
        entity_map = add_entities model, fragment['entities']
        add_reverse_foreign_keys entity_map, fragment['entities']

        model
      end

      private

      # Reconstruct entities and add them to the given model
      def add_entities(model, entity_fragment)
        # Recreate all the entities
        entity_map = {}
        entity_fragment.each do |entity_hash|
          entity_map[entity_hash['name']] = Entity.new entity_hash['name']
        end

        # Populate the entities and add them to the workload
        entities = EntityRepresenter.represent([])
        entities = entities.from_hash entity_fragment,
                                      user_options: { entity_map: entity_map }
        entities.each { |entity| model.add_entity entity }

        entity_map
      end

      # Add all the reverse foreign keys
      # @return [void]
      def add_reverse_foreign_keys(entity_map, entity_fragment)
        entity_fragment.each do |entity|
          entity['fields'].each do |field_hash|
            if field_hash['type'] == 'foreign_key'
              field = entity_map[entity['name']] \
                      .foreign_keys[field_hash['name']]
              field.reverse = field.entity.foreign_keys[field_hash['reverse']]
              field.instance_variable_set :@relationship,
                                          field_hash['relationship'].to_sym
            end
            field.freeze
          end
        end
      end
    end

    # Reconstruct the steps of a query plan
    class QueryPlanBuilder
      include Uber::Callable

      def call(_, represented:, fragment:, **)
        workload = represented.workload

        return nil if fragment.nil?

        if fragment['query'].nil?
          query = OpenStruct.new group: fragment['group']
          state = nil
        else
          query = Statement.parse fragment['query'], workload.model,
                                  group: fragment['group']
          state = Plans::QueryState.new query, workload
        end

        plan = build_plan query, represented.cost_model, fragment
        add_plan_steps plan, workload, fragment['steps'], represented.indexes,
                       state

        plan
      end

      private

      # Build a new query plan
      # @return [Plans::QueryPlan]
      def build_plan(query, cost_model, fragment)
        plan = Plans::QueryPlan.new query, cost_model

        plan.instance_variable_set(:@name, fragment['name']) \
          unless fragment['name'].nil?
        plan.instance_variable_set(:@weight, fragment['weight'])

        plan
      end

      # Loop over all steps in the plan and reconstruct them
      # @return [void]
      def add_plan_steps(plan, workload, steps_fragment, indexes, state)
        parent = Plans::RootPlanStep.new state
        f = ->(field) { workload.model[field['parent']][field['name']] }

        steps_fragment.each do |step_hash|
          step = build_step step_hash, state, parent, indexes, f
          rebuild_step_state step, step_hash
          plan << step
          parent = step
        end
      end

      # Rebuild a step from a hash using the given set of indexes
      # The final parameter is a function which maps field names to instances
      # @return [Plans::PlanStep]
      def build_step(step_hash, state, parent, indexes, f)
        send "build_#{step_hash['type']}_step".to_sym,
             step_hash, state, parent, indexes, f
      end

      # Rebuild a limit step
      # @return [Plans::LimitPlanStep]
      def build_limit_step(step_hash, _state, parent, _indexes, _f)
        limit = step_hash['limit'].to_i
        Plans::LimitPlanStep.new limit, parent.state
      end

      # Rebuild a sort step
      # @return [Plans::SortPlanStep]
      def build_sort_step(step_hash, _state, parent, _indexes, f)
        sort_fields = step_hash['sort_fields'].map(&f)
        Plans::SortPlanStep.new sort_fields, parent.state
      end

      # Rebuild a filter step
      # @return [Plans::FilterPlanStep]
      def build_filter_step(step_hash, _state, parent, _indexes, f)
        eq = step_hash['eq'].map(&f)
        range = f.call(step_hash['range']) if step_hash['range']
        Plans::FilterPlanStep.new eq, range, parent.state
      end

      def build_aggregation_step(step_hash, _state, parent, _indexes, f)
        counts = step_hash['counts'].map(&f)
        sums = step_hash['sums'].map(&f)
        avgs = step_hash['avgs'].map(&f)
        maxes = step_hash['maxes'].map(&f)
        groupby = step_hash['groupby'].map(&f)
        Plans::AggregationPlanStep.new(counts, sums, avgs, maxes, groupby, parent.state)
      end

      # Rebuild an index lookup step
      # @return [Plans::IndexLookupPlanStep]
      def build_index_lookup_step(step_hash, state, parent, indexes, f)
        index_key = step_hash['index']['key']
        step_index = indexes.find { |i| i.key == index_key }
        step = Plans::IndexLookupPlanStep.new step_index, state, parent
        add_index_lookup_filters step, step_hash, f

        order_by = (step_hash['order_by'] || []).map(&f)
        step.instance_variable_set(:@order_by, order_by)

        limit = step_hash['limit']
        step.instance_variable_set(:@limit, limit.to_i) unless limit.nil?

        step
      end

      # Add filters to a constructed index lookup step
      # @return [void]
      def add_index_lookup_filters(step, step_hash, f)
        eq_filter = (step_hash['eq_filter'] || []).map(&f)
        step.instance_variable_set(:@eq_filter, eq_filter)

        range_filter = step_hash['range_filter']
        range_filter = f.call(range_filter) unless range_filter.nil?
        step.instance_variable_set(:@range_filter, range_filter)
      end

      # Rebuild the state of the step from the provided hash
      # @return [void]
      def rebuild_step_state(step, step_hash)
        return if step.state.nil?

        # Copy the correct cardinality
        # XXX This may not preserve all the necessary state
        state = step.state.dup
        state.instance_variable_set :@cardinality, step_hash['cardinality']
        step.instance_variable_set :@cost, step_hash['cost']
        step.state = state.freeze
      end
    end

    class BaseSearchResultRepresenter < Representable::Decorator
      include Representable::JSON
      include Representable::YAML
      include Representable::Hash
      include Representable::Uncached

      extend Forwardable

      delegate :revision= => :represented
      delegate :command= => :represented

      property :model, decorator: ModelRepresenter,
               class: Model,
               deserialize: ModelBuilder.new

      collection :enumerated_indexes, decorator: FullIndexRepresenter,
                 class: Object,
                 deserialize: IndexBuilder.new

      # The backend cost model used to generate the schema
      # @return [Hash]
      def cost_model
        options = represented.cost_model.instance_variable_get(:@options)
        options[:name] = represented.cost_model.subtype_name
        options
      end

      # Look up the cost model by name and attach to the results
      # @return [void]
      def cost_model=(options)
        options = options.deep_symbolize_keys
        cost_model_class = Cost::Cost.subtype_class(options[:name])
        represented.cost_model = cost_model_class.new(**options)
      end

      property :cost_model, exec_context: :decorator

      # Include the revision of the code used to generate this output
      # @return [String]
      def revision
        `git rev-parse HEAD 2> /dev/null`.strip
      end

      property :revision, exec_context: :decorator

      # The time the results were generated
      # @return [Time]
      def time
        Time.now.rfc2822
      end

      # Reconstruct the time object from the timestamp
      # @return [void]
      def time=(time)
        represented.time = Time.rfc2822 time
      end

      property :time, exec_context: :decorator

      # The full command used to generate the results
      # @return [String]
      def command
        "#{$PROGRAM_NAME} #{ARGV.join ' '}"
      end

      property :command, exec_context: :decorator
    end

    class PreparePlanRepresenter < Representable::Decorator
      include Representable::JSON
      include Representable::YAML
      include Representable::Hash
      include Representable::Uncached

      property :index, decorator: FullIndexRepresenter, deserialize: IndexBuilder.new
      property :query_plan, decorator: QueryPlanRepresenter, deserialize: QueryPlanBuilder.new
      property :timestep
    end

    class MigratePlanRepresenter < Representable::Decorator
      include Representable::JSON
      include Representable::YAML
      include Representable::Hash
      include Representable::Uncached

      property :new_plan, decorator: QueryPlanRepresenter, class: Plans::QueryPlan, deserialize: QueryPlanBuilder.new
      property :obsolete_plan, decorator: QueryPlanRepresenter, class: Plans::QueryPlan, deserialize: QueryPlanBuilder.new
      property :query, decorator: StatementRepresenter
      property :start_time
      property :end_time
      collection :prepare_plans, decorator: PreparePlanRepresenter, class: Plans::MigratePreparePlan
    end

    # Represent results of a search operation
    class SearchResultRepresenter < BaseSearchResultRepresenter
      property :workload, decorator: WorkloadRepresenter,
               class: Workload,
               deserialize: WorkloadBuilder.new
      collection :indexes, decorator: FullIndexRepresenter,
                 class: Object,
                 deserialize: IndexBuilder.new

      collection :plans, decorator: QueryPlanRepresenter,
                 class: Object,
                 deserialize: QueryPlanBuilder.new
      collection :update_plans, decorator: UpdatePlanRepresenter,
                 class: Object,
                 deserialize: UpdatePlanBuilder.new
      property :total_size
      property :total_cost
    end

    class SearchTimeDependResultRepresenter < BaseSearchResultRepresenter
      property :timesteps
      property :workload, decorator: TimeDependWorkloadRepresenter,
               class: TimeDependWorkload,
               deserialize: WorkloadBuilder.new

      property :time_depend_indexes, class: Object, decorator: TimeDependIndexesRepsenter, deserialize: TimeDependIndexesBuilder.new

      collection :time_depend_plans, decorator: TimeDependPlanRepresenter,
                 class: Object,
                 deserialize: TimeDependPlanBuilder.new

      collection :time_depend_update_plans, decorator: TimeDependUpdatePlanRepresenter,
                 class: Object,
                 deserialize: TimeDependUpdatePlanBuilder.new

      collection :migrate_plans, decorator: MigratePlanRepresenter,
                 class: Object,
                 deserialize: MigratePlanBuilder.new

      collection :total_size
      collection :each_total_cost
    end
  end
end
