# frozen_string_literal: true

require 'logging'

module NoSE
  # Produces potential indices to be used in schemas
  class PatternMiner
    attr_reader :patterns

    def initialize
      @patterns = []
    end

    def pattern_for_workload(workload)
      queries = workload.statement_weights.keys
      queries.each do |query|
        unless query.groupby.empty?
          # group by fields should be orders in strict order
          query.groupby.each_slice(2) do |gl, gr|
            next if gr.nil?
            @patterns << Pattern.new(query, Set.new([gl]), Set.new([gr]))
          end

          (query.eq_fields - query.groupby).each do |hash_field|
            query.groupby.each do |grpby|
              @patterns << Pattern.new(query, Set.new([grpby]), Set.new([hash_field]))
            end
          end
        end

        (query.select - query.eq_fields).each do |v|
          @patterns << Pattern.new(query, Set.new(query.eq_fields), Set.new([v]))
        end
      end

      @patterns = choose_patterns workload.statement_weights
    end

    def validate_indexes(indexes)
      indexes.select do |index|
        @patterns.all? do |pattern|
          is_valid = true
          if index.hash_fields >= pattern.left
            is_valid = (index.order_fields.to_set + index.extra).to_set >= pattern.right
          end
          if index.order_fields.to_set >= pattern.left
            is_valid = index.extra >= pattern.right
          end
          is_valid
        end
      end
    end

    private

    def choose_patterns(statement_weights)
      weights = statement_weights.values
      threshold = weights.sort().reverse().slice([(weights.size / 2).floor, 0].max())
      patternable_queries = statement_weights.select{|_, v| v > threshold}.keys
      @patterns.select{|p| patternable_queries.include? p.query}
    end

    class Pattern
      attr_reader :query, :left, :right

      def initialize(query, left, right)
        @query = query
        @left = left
        @right = right
      end

      def to_s
        "#{@left} => #{@right}"
      end
    end
  end
end

