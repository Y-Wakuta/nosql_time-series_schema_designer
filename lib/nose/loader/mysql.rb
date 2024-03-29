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
               skip_existing = true, num_iterations = 100)
        indexes.map!(&:to_id_graph).uniq! if @backend.by_id_graph

        # MySQL query that joins many entites tend to take longer time.
        # Therefore, execute these queries first in parallel to reduce the total time
        indexes = indexes.sort_by { |idx | idx.graph.entities.size }.reverse

        # XXX Assuming backend is thread-safe
        #indexes.each do |index|
        Parallel.map(indexes, in_processes: Parallel.processor_count / 6) do |index|
          Hash[index, load_index(index, config, show_progress, limit, skip_existing, num_iterations,false)]
        end.inject(&:merge)
      end

      def query_for_indexes(indexes, config, does_outer_join: false, limit: nil)
        index_records = {}
        Parallel.each(indexes, in_threads: Parallel.processor_count / 5) do |index|
          index_records[index] = query_for_index(index, config, does_outer_join, limit)
        end
        index_records
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

      def query_for_index_full_outer_join(index, limit, config)
        starting = Time.now
        client = new_client config
        sql, fields = index_sql index, limit: limit, reverse_entities: false, full_outer_join: true
        results = execute_sql client, sql, fields, index

        reversed_sql, fields = index_sql index, limit: limit, reverse_entities: true
        unless sql == reversed_sql
          reversed_result = execute_sql client, reversed_sql, fields, index
          results += reversed_result
          results.uniq!
        end
        STDERR.puts "collect #{results.size} records with #{Time.now - starting} seconds from MySQL"

        results
      end

      def query_for_index_inner_join(index, limit, config)
        starting = Time.now
        client = new_client config
        sql, fields = index_sql index, limit: limit, reverse_entities: false, full_outer_join: false
        begin
          results = execute_sql client, sql, fields, index
        rescue Exception => e
          client = new_client config
          STDERR.puts "querying fail #{e.inspect}"
          sleep 10
          retry
        end

        STDERR.puts "collect #{results.size} records with #{Time.now - starting} seconds from MySQL"

        results
      end

      def query_for_index(index, config, does_outer_join, limit = nil)
        does_outer_join ?
          query_for_index_full_outer_join(index, limit, config) :
          query_for_index_inner_join(index, limit, config)
      end

      def choose_sample_records(index, results, num_iterations)
        # reduce the total record size, since choosing inter-quartile records from whole records takes long time.
        reduced_record_size = [num_iterations * 500, results.size / 20].max
        results = results.sample(reduced_record_size, random: Object::Random.new(100))

        results = Backend::CassandraBackend.remove_any_null_place_holder_row results
        key_records = results.sort_by{|r| r.values.to_s}.sample(num_iterations, random: Object::Random.new(100))
        s = Time.now
        interquartile_results = get_records_in_interquartile_range index, results, key_records
        puts "get interquatile range records: #{Time.now - s}"

        interquartile_results.sort_by{|r| r.values.to_s}.sample(num_iterations, random: Object::Random.new(100))
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
                             database: config[:database],
                             read_timeout: 2_000
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
            throw e
          end
        else
          client.query(sql).map { |row| hash_from_row row, fields }
        end.to_a
      end

      # Load a single index into the backend
      # @return [void]
      def load_index(index, config, show_progress, limit, skip_existing, num_iterations, does_outer_join = false)
        @backend.initialize_client
        # Skip this index if it's not empty
        if skip_existing && !@backend.index_empty?(index)
          @logger.info "Skipping index #{index.inspect}" if show_progress
          return
        end
        @logger.info index.inspect if show_progress
        results = query_for_index index, config, does_outer_join, limit
        @backend.load_index_by_cassandra_loader(index, results)

        return results if num_iterations < 0
        choose_sample_records index, results, num_iterations
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
      def index_sql_tables_outer_join(index, reverse_entities)
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
            cond = "#{key.parent.name}.#{key.name}=" \
              "#{key.entity.name}.#{key.entity.id_field.name}"
            get_condition_for_composite_key cond, key
          end.compact.join(' AND ')
        end
        tables
      end

      def index_sql_tables_inner_join(index)
        # Create JOIN statements
        tables = index.graph.entities.map(&:name).join ' JOIN '
        return tables if index.graph.size == 1

        tables << ' WHERE '
        tables << index.path.each_cons(2).map do |_prev_key, key|
          key = key.reverse if key.relationship == :many
          cond = "#{key.parent.name}.#{key.name}=" \
          "#{key.entity.name}.#{key.entity.id_field.name}"
          get_condition_for_composite_key cond, key
        end.join(' AND ')

        tables
      end

      def get_condition_for_composite_key(cond, key)
          if !key.composite_keys.nil? && !key.composite_keys.empty?
            key.composite_keys.each do |composite_key|
              cond += " AND #{composite_key["name"].parent.name}.#{composite_key["name"].name} =" \
                      " #{composite_key["related_key"].parent.name}.#{composite_key["related_key"].name}"
            end
          end
          cond
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
      def index_sql(index, limit: nil, reverse_entities: false, full_outer_join: true)
        # Get all the necessary fields
        fields, select = index_sql_select index

        # Construct the join condition
        tables = full_outer_join ?
                   index_sql_tables_outer_join(index, reverse_entities) :
                   index_sql_tables_inner_join(index)

        # if all field have the same value, the value will distinguished.
        # Therefore reduce the number of records here
        query = "SELECT DISTINCT #{select.join ', '} FROM #{tables}"

        # add ORDER BY to keep record order
        primary_keys_for_orderby = index.graph.entities.map(&:fields).reduce(&:merge)
                            .select{|_, v| v.primary_key? || v.instance_of?(Fields::CompositeKeyField)}
                            .map{|_, v| v.id}
                            .sort_by{|v| v}
        query += " ORDER BY #{primary_keys_for_orderby.join(", ")} "

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

      def get_records_in_interquartile_range(index, records, key_records)
        records_within_interquartile = []
        key_related_records = key_records.map{|kr| records.select{|r| index.hash_fields.all?{|hf| r[hf.id] == kr[hf.id]}}}
        key_related_records.each do |rows|
          if rows.size < 100
            records_within_interquartile += rows
            next
          end

          left_quartile, right_quartile = rows.size * 0.25, rows.size * 0.75
          records_within_interquartile += rows.sort_by{|r| index.order_fields.map{|of| r[of.id]}}[left_quartile...right_quartile]
        end
        records_within_interquartile
      end
    end
  end
end
