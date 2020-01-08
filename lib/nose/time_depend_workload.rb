# frozen_string_literal: true

require_relative 'model'
require_relative 'parser'

require 'erb'

module NoSE
  # A representation of a query workload over a given set of entities
  class TimeDependWorkload < Workload

    attr_accessor :timesteps, :interval, :is_static, :time_depend_statement_weights, :include_migration_cost

    def initialize(model = nil, &block)
      @time_depend_statement_weights = { default: {} }
      @model = model || Model.new
      @mix = :default
      @interval = 3600 # set seconds in an hour as default
      @is_static = false
      @include_migration_cost = true

      # Apply the DSL
      TimeDependWorkloadDSL.new(self).instance_eval(&block) if block_given?
    end

    # Add a new {Statement} to the workload or parse a string
    # @return [void]
    def add_statement(statement, mixes = {}, group: nil, label: nil, frequency: nil)
      statement = Statement.parse(statement, @model,
                                  group: group, label: label) \
        if statement.is_a? String
      statement.freeze

      mixes = { default: mixes } if mixes.is_a? Numeric
      mixes = { default: [1.0] * @timesteps } if mixes.empty?
      mixes.each do |mix, weight|
        @time_depend_statement_weights[mix] = {} unless @time_depend_statement_weights.key? mix
        fail "Frequency is required for #{statement.text}" if weight.nil? and frequency.nil?
        fail "number of Frequency should be same as timesteps for #{statement.text}" \
          unless weight.size == @timesteps or frequency&.size == @timestep
        fail "Frequency cannot become 0 for #{statement.text}" if weight.include?(0) or frequency&.include?(0)
        # ensure that all query has the same # of timesteps
        fail if @time_depend_statement_weights[mix].map{|_, weights| weights.size}.uniq.size > 1
        frequencies = (frequency.nil? ? weight : frequency).map{|f| f * @interval}
        @time_depend_statement_weights[mix][statement] = frequencies
      end

      sync_statement_weights
    end

    def sync_statement_weights
      # deep copy the weight hash
      @statement_weights = Marshal.load(Marshal.dump(@time_depend_statement_weights))

      if @is_static # if this workload is static workload, overwrite the value of frequency by the average frequency
        @time_depend_statement_weights.each do |mix, statements|
          statements.each do |statement, frequencies|
            @statement_weights[mix][statement] = average_array(frequencies)
          end
        end
      end
    end

    # Get all the support queries for updates in the workload
    # @return[Array<Statement>]
    def migrate_support_queries(index)
      # Get all fields which need to be selected by support queries
      select = index.all_fields
      return [] if select.empty?

      # Build conditions by traversing the foreign keys
      conditions = (index.hash_fields + index.order_fields).map do |c|
        next unless index.graph.entities.include? c.parent

        Condition.new c.parent.id_field, '='.to_sym, nil
      end.compact
      conditions = Hash[conditions.map do |condition|
        [condition.field.id, condition]
      end]

      params = {
        select: select,
        graph: index.graph,
        key_path: index.graph.longest_path,
        entity: index.graph.entities,
        conditions: conditions
      }
      query = Query.new(params, nil, group: "PrepareQuery")
      query.set_text
      query
    end

    def time_depend_statement_weights
      @time_depend_statement_weights[@mix]
    end

    def time_depend_statement_weights=(statement_weights)
      @time_depend_statement_weights[@mix] = statement_weights
      sync_statement_weights
    end

    # Strip the weights from the query dictionary and return a list of updates
    # @return [Array<Statement>]
    def updates
      @time_depend_statement_weights[@mix].keys.reject do |statement|
        statement.is_a? Query
      end
    end

    private

    def average_array(values)
      [values.sum / values.length] * values.length
    end
  end


  class TimeDependWorkloadDSL < WorkloadDSL

    def TimeSteps(timestep)
      @workload.timesteps = timestep
    end

    def Interval(seconds)
      @workload.interval = seconds
    end

    def Static(is_static)
      puts "\e[31mexecute optimization for average weight\e[0m"
      @workload.is_static = is_static
    end

    def IncludeMigrationCost(include_migration_cost)
      puts "ignore migration cost" unless include_migration_cost
      @workload.include_migration_cost = include_migration_cost
    end
  end
end
