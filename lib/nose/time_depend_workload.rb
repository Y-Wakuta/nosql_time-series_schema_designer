# frozen_string_literal: true

require_relative 'model'
require_relative 'parser'

require 'erb'

module NoSE
  # A representation of a query workload over a given set of entities
  class TimeDependWorkload < Workload

    attr_accessor :timesteps, :interval, :is_static, :time_depend_statement_weights,
                  :include_migration_cost, :creation_coeff, :migrate_support_coeff,
                  :start_workload_set, :end_workload_set, :start_workload_ratio, :end_workload_ratio,
                  :definition_type

    def initialize(model = nil, &block)
      @time_depend_statement_weights = { default: {} }
      @model = model || Model.new
      @mix = :default
      @interval = 60 # set minutes in an hour as default
      warn "The interval should be set in minutes" if @interval > 1000
      @creation_coeff = 0.001
      @migrate_support_coeff = 0.001
      @is_static = false
      @include_migration_cost = true
      @definition_type = DEFINITION_TYPE::FLOAT_ARRAY

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
      if @definition_type == DEFINITION_TYPE::FLOAT_ARRAY
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

          print group&.rjust(22)
          print (" " + mix.to_s + " ").rjust(22)
          print frequencies.map{|f| f.round.to_s.rjust(10)}
          puts ""

        end
      elsif @definition_type == DEFINITION_TYPE::WORKLOAD_SET_RATIO
        fail "required field is not given" if @start_workload_ratio.nil? or @end_workload_ratio.nil? \
                                              or @start_workload_set.nil? or @end_workload_set.nil?
        mixes[@start_workload_set] = 0 if mixes[@start_workload_set].nil?
        mixes[@end_workload_set] = 0 if mixes[@end_workload_set].nil?

        @time_depend_statement_weights[@mix] = {} unless @time_depend_statement_weights.key? @mix
        @time_depend_statement_weights[@mix][statement] = calculate_td_frequency_by_ratio(mixes[@start_workload_set],
                                                                                                  @start_workload_ratio,
                                                                                                  mixes[@end_workload_set],
                                                                                                  @end_workload_ratio,
                                                                                                  @timesteps)
      else
        fail "DEFINITION_TYPE is required"
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

    def calculate_td_frequency_by_ratio(start_mix_freq, start_ratio, end_mix_freq, end_ratio, timestep)
      fail "Sum of ratios must be 1.0" unless (1.0 - start_ratio - end_ratio).abs < 0.001
      start_freq = start_mix_freq * start_ratio + end_mix_freq * end_ratio
      end_freq = start_mix_freq * end_ratio + end_mix_freq * start_ratio
      step_size = (end_freq - start_freq) / (timestep - 1)
      (0...timestep).map do |t|
        t * step_size + start_freq
      end.to_a.map{|f| BigDecimal((f * @interval).to_s).ceil(3).to_f}
    end
  end


  class TimeDependWorkloadDSL < WorkloadDSL

    def DefaultMix(mix)
      fail "DefaultMix method cannot be used in #{DEFINITION_TYPE::WORKLOAD_SET_RATIO} TimeDependWorkload" \
        if @workload.definition_type == DEFINITION_TYPE::WORKLOAD_SET_RATIO
      super(mix)
    end

    def DefinitionType(definition_type)
      @workload.definition_type = definition_type
    end

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
      puts "ignore migration cost. NOTE: This option does not literally ignore migration cost. "\
           "This option changes each migration cost drastically smaller" unless include_migration_cost
      @workload.include_migration_cost = include_migration_cost
    end

    # cost for creating new column family in migration process
    def CreationCoeff(creation_coeff)
      @workload.creation_coeff = creation_coeff
    end

    # cost for preparing data for new column families
    def MigrateSupportCoeff(migrate_support_coeff)
      @workload.migrate_support_coeff = migrate_support_coeff
    end

    def StartWorkloadSet(start_workload_set, first_ratio)
      @workload.start_workload_set = start_workload_set
      @workload.start_workload_ratio = first_ratio
    end

    def EndWorkloadSet(end_workload_set, first_ratio)
      @workload.end_workload_set = end_workload_set
      @workload.end_workload_ratio = first_ratio
    end
  end

  module DEFINITION_TYPE
    FLOAT_ARRAY = "FLOAT_ARRAY".freeze
    WORKLOAD_SET_RATIO = "WORKLOAD_SET_RATIO".freeze
  end
end
