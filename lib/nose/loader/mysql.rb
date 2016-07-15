# This is optional so other things can run under JRuby,
# however this loader won't work so we need to use MRI
begin
  require 'mysql2'
rescue LoadError
  require 'mysql'
end

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
        # XXX Assuming backend is thread-safe
        Parallel.each(indexes, in_threads: 2) do |index|
          client = new_client config

          # Skip this index if it's not empty
          if skip_existing && !@backend.index_empty?(index)
            @logger.info "Skipping index #{index.inspect}" if show_progress
            next
          end
          @logger.info "#{index.inspect}" if show_progress

          sql, fields = index_sql index, limit
          if @query_options
            results = client.query(sql, **@query_options)
          else
            results = client.query(sql).map do |row|
              row_hash = {}
              fields.each_with_index do |field, i|
                value = field.class.value_from_string row[i]
                row_hash[field.id] = value
              end

              row_hash
            end
          end

          result_chunk = []
          results.each do |result|
            result_chunk.push result
            if result_chunk.length >= 100
              @backend.index_insert_chunk index, result_chunk
              result_chunk = []
            end
          end
          @backend.index_insert_chunk index, result_chunk \
            unless result_chunk.empty?
        end
      end

      # Read all tables in the database and construct a workload object
      def workload(config)
        client = new_client config

        workload = Workload.new
        if @array_options
          results = client.query('SHOW TABLES').each(**@array_options)
        else
          results = client.query('SHOW TABLES').each
        end
        results.each do |table, *|
          entity = Entity.new table
          count = client.query("SELECT COUNT(*) FROM #{table}").first
          entity.count = count.is_a?(Hash) ? count.values.first : count

          if @array_options
            describe = client.query("DESCRIBE #{table}").each(**@array_options)
          else
            describe = client.query("DESCRIBE #{table}").each
          end
          describe.each do |name, type, _, key, _, _|
            if key == 'PRI'
              field_class = Fields::IDField
            else
              case type
              when /datetime/
                field_class = Fields::DateField
              when /float/
                field_class = Fields::FloatField
              when /text/
                # TODO: Get length
                field_class = Fields::StringField
              when /varchar\(([0-9]+)\)/
                # TODO: Use length
                field_class = Fields::StringField
              when /(tiny)?int/
                field_class = Fields::IntegerField
              end
            end

            entity << field_class.new(name)
          end

          workload << entity
          # TODO: Handle foreign keys
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

      # Get all the fields selected by this index
      def index_sql_select(index)
        fields = index.hash_fields.to_a + index.order_fields + index.extra.to_a
        fields << index.path.entities.last.id_field

        [fields, fields.map do |field|
          "#{field.parent.name}.#{field.name} AS " \
          "#{field.parent.name}_#{field.name}"
        end]
      end

      # Get the list of tables along with the join condition
      # for a query to fetch index data
      # @return [String]
      def index_sql_tables(index)
        # Create JOIN statements
        tables = index.path.entities.map(&:name).join ' JOIN '
        return tables if index.path.length == 1

        tables += ' WHERE '
        tables += index.path.each_cons(2).map do |_prev_key, key|
          key = key.reverse if key.relationship == :many
          "#{key.parent.name}.#{key.name}=" \
            "#{key.entity.name}.#{key.entity.id_field.name}"
        end.join ' AND '

        tables
      end

      # Construct a SQL statement to fetch the data to populate this index
      # @return [String]
      def index_sql(index, limit = nil)
        # Get all the necessary fields
        fields, select = index_sql_select index

        # Construct the join condition
        tables = index_sql_tables index

        query = "SELECT #{select.join ', '} FROM #{tables}"
        query += " LIMIT #{limit}" unless limit.nil?

        @logger.debug query
        [query, fields]
      end
    end
  end
end
