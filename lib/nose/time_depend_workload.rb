# frozen_string_literal: true

require_relative 'model'
require_relative 'parser'

require 'erb'

module NoSE
  # A representation of a query workload over a given set of entities
  class TimeDependWorkload < Workload

    attr_accessor :timesteps, :interval

    def initialize(model = nil, &block)
      @statement_weights = { default: {} }
      @model = model || Model.new
      @mix = :default
      @interval = 3600 # set seconds in an hour as default

      # Apply the DSL
      TimeDependWorkloadDSL.new(self).instance_eval(&block) if block_given?
    end

    # Add a new {Statement} to the workload or parse a string
    # @return [void]
    def add_statement(statement, mixes = {}, group: nil, label: nil)
      statement = Statement.parse(statement, @model,
                                  group: group, label: label) \
        if statement.is_a? String
      statement.freeze

      mixes = { default: mixes } if mixes.is_a? Numeric
      mixes = { default: 1.0 } if mixes.empty?
      mixes.each do |mix, weight|
        @statement_weights[mix] = {} unless @statement_weights.key? mix
        fail "Frequency is required for #{statement.text}" if weight.nil?
        fail "number of Frequency should be same as timesteps for #{statement.text}" unless weight.size == timesteps
        fail "Frequency cannot become 0 for #{statement.text}" if weight.any?{|w| w == 0}
        @statement_weights[mix][statement] = weight.map{|f| f * @interval}
      end

      # ensure that all query has the same # of timesteps
      fail if @statement_weights[mix].map{|_, weights| weights.size}.uniq.size > 1
    end
  end


  class TimeDependWorkloadDSL < WorkloadDSL

    def TimeSteps(timestep)
      @workload.timesteps = timestep
    end

    def Interval(seconds)
      @workload.interval = seconds
    end
  end
end
