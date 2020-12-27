# frozen_string_literal: true

# This is optional so other things can run under JRuby,
# however this loader won't work so we need to use MRI
begin
  require 'mysql2'
rescue LoadError
  require 'mysql'
end
require 'etc'
require 'digest'

module NoSE
  module Loader
    # Load data from a MySQL database into a backend
    class MysqlLoader < LoaderBase
      def initialize(workload = nil, backend = nil)
        @logger = Logging.logger['nose::loader::mysqlloader']

        @workload = workload
        @backend = backend
      end

      # Load a generated set of indexes with data from MySQL
      def load(indexes, config, show_progress = false, limit = nil,
               skip_existing = true)
        indexes.map!(&:to_id_graph).uniq! if @backend.by_id_graph

        # XXX Assuming backend is thread-safe
        #indexes.each do |index|
        Parallel.each(indexes, in_processes: Parallel.processor_count / 5) do |index|
          load_index index, config, show_progress, limit, skip_existing
        end
      end

      def load_dummy(indexes, config, show_progress = false, limit = nil,
               skip_existing = true)
        indexes.map!(&:to_id_graph).uniq! if @backend.by_id_graph

        # XXX Assuming backend is thread-safe
        #Parallel.each(indexes, in_threads: 2) do |index|
        indexes.each do |index|
          load_index_dummy index, config, show_progress, limit, skip_existing
        end
      end

      # Read all tables in the database and construct a workload object
      def workload(config)
        client = new_client config

        workload = Workload.new
        results = if @array_options
                    client.query('SHOW TABLES').each(**@array_options)
                  else
                    client.query('SHOW TABLES').each
                  end

        results.each do |table, *|
          # TODO: Handle foreign keys
          workload << entity_for_table(client, table)
        end

        workload
      end

      private

      # Create a new client from the given configuration
      def new_client(config)
        if Object.const_defined?(:Mysql2)
          @query_options = { stream: true, cache_rows: false }
          @array_options = { as: :array }
          Mysql2::Client.new host: config[:host],
                             username: config[:username],
                             password: config[:password],
                             database: config[:database]
        else
          @query_options = false
          @array_options = false
          Mysql.connect config[:host], config[:username], config[:password],
                        config[:database]
        end
      end

      def execute_sql(client, sql, fields, index)
         if @query_options
           begin
             STDERR.puts sql
             client.query(sql, **@query_options)
           rescue => e
             STDERR.puts index.inspect
             #throw e
           end
         else
           client.query(sql).map { |row| hash_from_row row, fields }
         end.to_a
      end

      def query_for_index(client, index, limit)
        sql, fields = index_sql index, limit: limit, reverse_entities: false
        results = execute_sql client, sql, fields, index

        reversed_sql, fields = index_sql index, limit: limit, reverse_entities: true
        unless sql == reversed_sql
          reversed_result = execute_sql client, reversed_sql, fields, index
          results += reversed_result
          results.uniq!
        end

        results
      end

      # Load a single index into the backend
      # @return [void]
      def load_index(index, config, show_progress, limit, skip_existing)
        client = new_client config

        # Skip this index if it's not empty
        if skip_existing && !@backend.index_empty?(index)
          @logger.info "Skipping index #{index.inspect}" if show_progress
          return
        end
        @logger.info index.inspect if show_progress

        results = query_for_index client, index, limit
        @backend.load_index_by_COPY(index, results)
        #@backend.index_insert(index, results)
      end

      def load_index_dummy(index, config, show_progress, limit, skip_existing)
        client = new_client config

        @logger.info index.inspect if show_progress
        raw_results = query_for_index client, index, limit

        results_on_mysql = raw_results.map do |row|
          row.each do |f, v|
            current = index.all_fields.find{|field| field.id == f}
            if current.is_a?(NoSE::Fields::DateField)
              row[f] = v.to_time unless v.nil?
            end
            row[f] = @backend.index_row(row, [current]).first
          end
          row
        end
        results_on_backend = @backend.index_records(index, index.all_fields)
        results_on_backend.each {|r| r.delete('value_hash')}
        compare_two_results(index, results_on_mysql, results_on_backend)
      end

      def compare_two_results(index, left_hash, right_hash)
        fail 'result field does not match' unless left_hash.first.keys.to_set == right_hash.first.keys.to_set

        left_hash.first.keys.each do |field_name|
          left_values = left_hash.map{|lh| lh[field_name]}
          right_values = right_hash.map{|rh| rh[field_name]}
          if compare_values left_values.compact, right_values
            STDERR.puts "    #{field_name} in #{index.key} matches"
          else
            if compare_approximately_values left_values, right_values
              STDERR.puts "    === #{field_name} in #{index.key} approximately match #{left_values.size} <-> #{right_values.size}==="
              if left_values.size > right_values.size
                STDERR.puts "      left_values is larger than right_values : #{left_values.difference(right_values).map(&:to_s)}"
              else
                STDERR.puts "      right_values is larger than left_values : #{right_values.difference(left_values).map(&:to_s)}"
              end
            else
              # this possibly happen if there are INSERT or UPDATE
              STDERR.puts "    === #{field_name} in #{index.key} does not match ==="
              STDERR.puts "      #{left_values.size}: #{left_values.map(&:to_s).sort.take(10)}"
              STDERR.puts "      #{right_values.size}: #{right_values.map(&:to_s).sort.take(10)}"
              STDERR.puts "    ==========================================="
            end
          end
        end
      end

      def compare_values(left_values, right_values)
        target_class = left_values.first.class
        if target_class == Cassandra::Uuid
          left_values = left_values.map(&:to_s).sort
          right_values = right_values.map(&:to_s).sort
          return left_values == right_values
        end

        left_values.sort!
        right_values.sort!

        if target_class == Float
          return left_values.sort.zip(right_values.sort).map{|l, r| l - r}.all?{|i| i.abs < 0.001}
        elsif target_class == Time
          # TODO: fix: due to difference of timezone, the datetime value changes at each insertion and query. In this case, at least the time difference of all records are same

          return left_values.zip(right_values).map{|l, r| l - r}.uniq.size == 1
        end
        return left_values == right_values
      end

      def compare_approximately_values(left_values, right_values)
        return false if (left_values.size - right_values.size).abs > left_values.size * 0.1
        if left_values.first.class == Float
          difference = left_values.reject{|lv| right_values.any?{|rv| (lv - rv).abs < 0.01}}
          return false if difference.size > left_values.size * 0.1
          difference = right_values.reject{|lv| left_values.any?{|rv| (lv - rv).abs < 0.01}}
          return false if difference.size > left_values.size * 0.1
          return true
        end

        difference = left_values.difference(right_values)
        return false if difference.size > left_values.size * 0.1
        difference = right_values.difference(left_values)
        return false if difference.size > right_values.size * 0.1
        true
      end

      # Construct a hash from the given row returned by the client
      # @return [Hash]
      def hash_from_row(row, fields)
        row_hash = {}
        fields.each_with_index do |field, i|
          value = field.class.value_from_string row[i]
          row_hash[field.id] = value
        end

        row_hash
      end

      # Get all the fields selected by this index
      def index_sql_select(index)
        fields = index.hash_fields.to_a + index.order_fields + index.extra.to_a

        [fields, fields.map do |field|
          "#{field.parent.name}.#{field.name} AS " \
          "#{field.parent.name}_#{field.name}"
        end]
      end

      # Get the list of tables along with the join condition
      # for a query to fetch index data
      # @return [String]
      def index_sql_tables(index, reverse_entities)
        # Create JOIN statements
        return index.graph.entities.first.name if index.graph.size == 1

        entity_pairs = index.graph.entities_in_outer_join_order(reverse_entities)
        fact_table_candidates = entity_pairs.flat_map{|ep| ep[:from]}.to_set
        entity_pairs.each{|f| fact_table_candidates.delete(f[:to])}
        fact_table = fact_table_candidates.first

        reordered_entity_pairs = reorder_dimension_tables(entity_pairs, fact_table)

        tables = fact_table.name
        reordered_entity_pairs.each do |ep|
          if ep[:join] == "left"
            tables += ' LEFT OUTER JOIN ' + ep[:to].name
          else
            tables += ' RIGHT OUTER JOIN ' + ep[:from].name
          end

          tables << ' ON '
          tables << index.path.each_cons(2).map do |_prev_key, key|
            key = key.reverse if key.relationship == :many
            next unless Set.new([key.parent, key.entity]) ==  Set.new([ep[:from], ep[:to]])
            "#{key.parent.name}.#{key.name}=" \
              "#{key.entity.name}.#{key.entity.id_field.name}"
          end.compact.join(' AND ')
        end
        tables
      end

      def reorder_dimension_tables(entity_pairs, current_table)
        tmp_entity = current_table.dup
        tmp_entity_pairs = entity_pairs.dup

        temporaly_reordered = []
        loop do
          next_entity = tmp_entity_pairs.find{|ep| ep[:from] == tmp_entity}
          unless next_entity.nil?
            temporaly_reordered << next_entity
            tmp_entity_pairs.delete(next_entity)
            tmp_entity = next_entity[:to]
          else
            next_right_entity = tmp_entity_pairs.find{|ep| ep[:to] == tmp_entity}
            unless next_right_entity.nil?
              temporaly_reordered << next_right_entity
              tmp_entity_pairs.delete(next_right_entity)
              tmp_entity = next_right_entity[:from]
            end
          end

          break if tmp_entity_pairs.size == 0
        end

        temporaly_reordered.map do |ep|
          if ep[:from] == current_table
            ep[:join] = "left"
            current_table = ep[:to]
          elsif ep[:to] == current_table
            ep[:join] = "right"
            current_table = ep[:from]
          else
            fail "unrecognized join order"
          end
          ep
        end
      end

      # Construct a SQL statement to fetch the data to populate this index
      # @return [String]
      def index_sql(index, limit: nil, reverse_entities: false)
        # Get all the necessary fields
        fields, select = index_sql_select index

        # Construct the join condition
        tables = index_sql_tables index, reverse_entities

        # if all field have the same value, the value will distinguished.
        # Therefore reduce the number of records here
        query = "SELECT DISTINCT #{select.join ', '} FROM #{tables}"
        query += " LIMIT #{limit}" unless limit.nil?

        @logger.debug query
        [query, fields]
      end

      # Generate an entity definition from a given table
      # @return [Entity]
      def entity_for_table(client, table)
        entity = Entity.new table
        count = client.query("SELECT count(*) FROM #{table}").first
        entity.count = count.is_a?(Hash) ? count.values.first : count

        describe = if @array_options
                     client.query("DESCRIBE #{table}").each(**@array_options)
                   else
                     client.query("DESCRIBE #{table}").each
                   end

        describe.each do |name, type, _, key|
          field_class = key == 'PRI' ? Fields::IDField : field_class(type)
          entity << field_class.new(name)
        end

        entity
      end

      # Produce the Ruby class used to represent a MySQL type
      # @return [Class]
      def field_class(type)
        case type
        when /datetime/
          Fields::DateField
        when /float/
          Fields::FloatField
        when /text/
          # TODO: Get length
          Fields::StringField
        when /varchar\(([0-9]+)\)/
          # TODO: Use length
          Fields::StringField
        when /(tiny)?int/
          Fields::IntegerField
        end
      end
    end
  end
end
