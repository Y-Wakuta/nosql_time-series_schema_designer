# frozen_string_literal: true

require_relative 'model'
require_relative 'parser'

require 'erb'

module NoSE
  # A representation of a query workload over a given set of entities
  class TimeDependWorkload < Workload

    attr_accessor :timesteps

    def initialize(model = nil, &block)
      @statement_weights = { default: {} }
      @model = model || Model.new
      @mix = :default

      set_dummy_functions

      # Apply the DSL
      TimeDependWorkloadDSL.new(self).instance_eval(&block) if block_given?
    end

    def set_dummy_functions
      @dummy_functions = {
        :increase => Proc.new{|seed| (1..@timesteps).map{|t| (0.1 * t + seed).round(2)}},
        :decrease => Proc.new{|seed| (1..@timesteps).map{|t| (-0.1 * t + seed).round(2)}},
        :static => Proc.new{|seed| (1..@timesteps).map{|_| seed}}
      }
    end

    # Add a new {Statement} to the workload or parse a string
    # @return [void]
    def add_statement(statement, mixes = {}, frequencies, function_type, group: nil, label: nil)
      statement = Statement.parse(statement, @model,
                                  group: group, label: label) \
        if statement.is_a? String
      statement.freeze

      mixes = { default: mixes } if mixes.is_a? Numeric
      mixes = { default: 1.0 } if mixes.empty?
      mixes.each do |mix, weight|
        @statement_weights[mix] = {} unless @statement_weights.key? mix
        @statement_weights[mix][statement] = frequencies.nil? ? @dummy_functions[function_type].call(weight) : frequencies
      end

      # ensure that all query has the same # of timesteps
      fail if @statement_weights[mix].map{|_, weights| weights.size}.uniq.size > 1
    end

  end


  class TimeDependWorkloadDSL < WorkloadDSL

    def Q(statement, weight = 1.0, frequencies, function_type, group: nil, label: nil, **mixes)
        fail 'Statements require a workload' if @workload.nil?

        return if weight.zero? && mixes.empty?
        mixes = { default: weight } if mixes.empty?
        @workload.add_statement statement, mixes, frequencies, function_type, group: group, label: label
      end

      # Allow grouping statements with an associated weight
      # @return [void]
      def Group(name, weight = 1.0, function_type = :static, **mixes, &block)
        fail 'Groups require a workload' if @workload.nil?

        # Apply the DSL
        dsl = TimeDependGroupDSL.new
        dsl.instance_eval(&block) if block_given?
        dsl.statements.each do |statement|
          frequency = dsl.frequencies&.has_key?(statement) ? dsl.frequencies[statement] : nil
          Q(statement, weight, frequency, function_type,**mixes, group: name)
        end
      end

      def TimeSteps(timestep)
        @workload.timesteps = timestep
      end
    end


  class TimeDependGroupDSL < GroupDSL
    attr_reader :frequencies
    # get frequcny array
    def Q(statement, freq = nil)
          @statements << statement
          return if freq.nil?

          @frequencies = {} if @frequencies.nil?
          fail if @frequencies.has_key? statement and @frequencies[statement] == freq
          @frequencies[statement] = freq
    end
  end
end
