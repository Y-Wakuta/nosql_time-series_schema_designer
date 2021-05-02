# frozen_string_literal: true

require 'cassandra'
require 'zlib'
require 'tmpdir'
require 'date'

module NoSE
  module Backend
    # A backend which communicates with Cassandra via CQL
    class CassandraBackend < Backend
      include Subtype

      @@value_place_holder = {
          :string => "_",
          :date => Time.at(0),
          :numeric => (-1.0e+5).to_i,
          :uuid => Cassandra::Uuid.new("cd9747a1-b59c-4aca-a12e-65915bcd2768")
      }

      def initialize(model, indexes, plans, update_plans, config)
        super

        @hosts = config[:hosts]
        @port = config[:port]
        @keyspace = config[:keyspace]
        @generator = Cassandra::Uuid::Generator.new
      end

      def self.remove_all_null_place_holder_row(rows)
        rows.to_a.select do |row|
          not row.values.all?{|f| @@value_place_holder.values.include? f}
        end
      end

      def self.remove_any_null_place_holder_row(rows)
        rows.to_a.select do |row|
          not row.values.any?{|f| @@value_place_holder.values.include? f}
        end
      end

      def self.remove_any_null_row(rows)
        rows.to_a.select do |row|
          not row.values.any?{|v| v.nil?}
        end
      end

      def self.convert_nil_2_place_holder(field)
        return @@value_place_holder[:string] if field.instance_of?(NoSE::Fields::StringField)
        return @@value_place_holder[:numeric].to_i if field.is_a?(NoSE::Fields::IntegerField)
        return @@value_place_holder[:numeric].to_f if field.is_a?(NoSE::Fields::FloatField)
        return @@value_place_holder[:date] if field.instance_of?(NoSE::Fields::DateField)
        return @@value_place_holder[:uuid] if field.instance_of?(NoSE::Fields::IDField) or field.instance_of?(NoSE::Fields::ForeignKeyField) or field.instance_of?(NoSE::Fields::CompositeKeyField)
        fail "#{field.inspect}, #{field.class} is still not supported"
      end

      # Generate a random UUID
      def generate_id
        @generator.uuid
      end

      def create_index(index, execute = false, skip_existing = false)
        ddl = index_cql index
        begin
          @client.execute(ddl) if execute
        rescue Cassandra::Errors::AlreadyExistsError => exc
          return if skip_existing

          new_exc = IndexAlreadyExists.new exc.message
          new_exc.set_backtrace exc.backtrace
          raise new_exc
        end

        ddl
      end

      def recreate_index(index, execute = false, skip_existing = false,
                         drop_existing = false)
        drop_index(index) if drop_existing && index_exists?(index)
        create_index(index, execute, skip_existing)
      end

      # Produce the DDL necessary for column families for the given indexes
      # and optionally execute them against the server
      def indexes_ddl(execute = false, skip_existing = false,
                      drop_existing = false)
        create_indexes(@indexes, execute, skip_existing, drop_existing)
      end

      # Produce the DDL necessary for column families for the given indexes
      # and optionally execute them against the server
      def create_indexes(indexes, execute = false, skip_existing = false,
                         drop_existing = false)
        indexes.map do |index|
          recreate_index(index, execute, skip_existing, drop_existing)
        end
      end

      def insert_cql(index)
        fields = index.all_fields.to_a
        "INSERT INTO \"#{index.key}\" (" \
                   "#{field_names fields}" \
                   ") VALUES (#{(['?'] * fields.length).join ', '})"
      end

      # Insert a chunk of rows into an index
      # @return [Array<Array<Cassandra::Uuid>>]
      def index_insert_chunk(index, chunk)
        fields = index.all_fields.to_a
        prepared = insert_cql index
        prepared = @client.prepare prepared

        ids = []
        batches = @client.batch do |batch|
          chunk.each do |row|
            index_row = index_row(row, fields)
            ids << (index.hash_fields.to_a + index.order_fields).map do |field|
              index_row[fields.index field]
            end
            batch.add prepared, arguments: index_row
          end
        end
        begin
          @client.execute(batches)
        rescue => e
          STDERR.puts "===========+==========" + e
          throw e
        end

        ids
      end

      def index_insert(index, results)
        STDERR.puts "load data to index: \e[35m#{index.key}: #{index.hash_str} \e[0m"
        fail "no data will be loaded on #{index.key}" if results.size == 0
        puts "insert through ruby driver: #{index.key}, #{results.size.to_s}"
        chunk_size = 3000
        inserted_size = 0
        Parallel.each(results.to_a.each_slice(chunk_size), in_threads: 10) do |result_chunk|
          index_insert_chunk index, result_chunk
          inserted_size += result_chunk.size
          STDERR.puts "inserted chunk #{index.key}:  #{inserted_size} / #{results.size}"
        end
      end

      # Check if the given index is empty
      def index_empty?(index)
        query = "SELECT count(*) FROM \"#{index.key}\" LIMIT 1"
        @client.execute(query).first.values.first.zero?
      end

      # Check if a given index exists in the target database
      def index_exists?(index)
        tables = @client.execute("SELECT table_name FROM system_schema.tables WHERE keyspace_name='#{@keyspace}'").to_a.map{|t| t.values}.flatten
        tables.include? index.key
      end

      # Check if a given index exists in the target database
      def drop_index(index)
        @client.execute "DROP TABLE \"#{index.key}\""
      end

      def clear_keyspace
        STDERR.puts "clearing keyspace: #{@keyspace}"
        @cluster.keyspace(@keyspace).tables.map(&:name).each do |index_key|
          @client.execute("DROP TABLE #{index_key}")
        end

        # this works only for localhost
        #puts `docker exec cassandra_migrate nodetool clearsnapshot`
      end

      # Sample a number of values from the given index
      def index_sample(index, count = nil)
        record_pool_magnification = 100
        field_list = index.all_fields.map { |f| "\"#{f.id}\"" }
        query = "SELECT #{field_list.join ', '} FROM \"#{index.key}\""
        rows = query_index_limit_for_sample query, count

        # XXX Ignore null values for now
        # fail if rows.any? { |row| row.values.any?(&:nil?) }

        return rows if count.nil?
        if rows.size == 0
          fail "collected record for #{index.key} was empty: #{index.hash_str}"
        end

        r = Object::Random.new(100)
        (0...(record_pool_magnification * count)).map{|_| r.rand(rows.size)}.uniq.map do |i|
          rows[i]
        end.compact.take(count)
      end

      def index_records(index, required_fields)
        field_list = (index.all_fields.to_set & required_fields.to_set).to_a.map { |f| "\"#{f.id}\"" }
        #field_list = (field_list.to_set & required_fields.map{|f| "\"#{f.id}\""}).to_a
        query = "SELECT #{field_list.join ', '} FROM \"#{index.key}\""

        query_index query
      end

      def load_index_by_cassandra_loader(index, results)
        starting = Time.now.utc
        fields = index.all_fields.to_a
        columns = fields.map(&:id)
        columns << "value_hash"
        formatted_result = format_result index, results

        fail 'no data given to load on cassandra' if formatted_result.size == 0
        inserting_try = 0
        host_name = ""
        begin
          Parallel.each_with_index(formatted_result.each_slice(1_000_000), in_processes: 5) do |results_chunk, idx|
            #formatted_result.each_slice(10_000_000).each_with_index do |results_chunk, idx|
            host_name = @hosts.sample(1).first
            Dir.mktmpdir do |dir|
              file_name = "#{dir}/#{index.key}_#{idx}.csv"
              g = File.open(file_name, "w") do |f|
                f.puts(columns.join('|').to_s)
                results_chunk.each {|row| f.puts(row.join('|'))}
                f
              end
              STDERR.puts "  insert through csv: #{index.key}, #{file_name}, #{results_chunk.size.to_s}"
              ENV['CQLSH_NO_BUNDLED'] = 'TRUE'
              ret = system("./cassandra-loader -badDir /tmp/ -queryTimeout 20 -numRetries 5 -batchSize 20 " \
                           "-dateFormat \"yyyy-MM-dd\" -skipRows 1 -delim \"|\" -f #{file_name} -host #{host_name} " \
                           "-schema \"#{@keyspace}.#{index.key}(#{columns.join(',').to_s})\" > /dev/null")

              fail "  loading error detected: #{index.key}" unless ret
              g.close
            end
          end
        rescue
          STDERR.puts "  loading error detected for #{index.key}"
          sleep 30 if results.size > 100_000
          all_retries = 10
          if inserting_try < all_retries
            STDERR.puts "  once truncate record of #{index.key}"
            ret = system("cqlsh --request-timeout=10000 #{host_name} -k #{@keyspace} -e \"TRUNCATE #{index.key}\"")
            if ret
              STDERR.puts "  truncate succeeded"
              STDERR.puts "  retry inserting #{inserting_try} / #{all_retries}"
              inserting_try += 1
              sleep 30
              retry
            end
            retry
          end
        end
        GC.start
        ending = Time.now.utc
        STDERR.puts "loading through csv time: #{ending - starting} for #{results.size.to_s} records on #{index.key}"
      end

      def unload_index_by_cassandra_unloader(index)
        starting = Time.now.utc
        columns = index.all_fields.map(&:id)

        inserting_try = 0
        records = nil
        Dir.mktmpdir do |dir|
          file_name = "#{dir}/#{index.key}.csv"
          host_name = @hosts.sample(1).first
          ENV['CQLSH_NO_BUNDLED'] = 'TRUE'
          begin
            hoge_time = Time.now
            ret = system("./cassandra-unloader -f stdout -dateFormat \"yyyy-MM-dd\" -delim \"|\" -host #{host_name} " \
                         "-schema \"#{@keyspace}.#{index.key}(#{columns.join(',').to_s})\" > #{file_name}")
            STDERR.puts "  unloading time for #{index.key} was #{Time.now - hoge_time}"
            fail "data collecting error detected: #{index.key}" unless ret
          rescue Exception => e
            puts e
            STDERR.puts "  query error detected for #{index.key}"
            #sleep 30 if results.size > 100_000
            all_retries = 10
            if inserting_try < all_retries
              STDERR.puts "  retry query #{inserting_try} / #{all_retries}"
              inserting_try += 1
              sleep 30
              retry
            end
          end
          records = CSV.open(file_name,col_sep: "|", headers: columns).map(&:to_h)
        end
        ending = Time.now.utc
        STDERR.puts "unloading through csv time: #{ending - starting} for #{records.size.to_s} records on #{index.key}"
        records
      end

      # Produce an array of fields in the correct order for a CQL insert
      # @return [Array]
      def index_row(row, fields)
        fields.map do |field|
          value = row[field.id]
          value = convert_id_2_uuid value if field.is_a?(Fields::IDField)
          value = value.to_f if value.instance_of?(BigDecimal)
          value = CassandraBackend.convert_nil_2_place_holder(field) if value.nil?
          value
        end
      end

      def self.create_empty_record(index)
        row = {}
        index.all_fields.each do |f|
          row[f.id] = CassandraBackend.convert_nil_2_place_holder f
        end
        row
      end

      # Produce the CQL to create the definition for a given index
      # @return [String]
      def index_cql(index)
        ddl = "CREATE COLUMNFAMILY \"#{index.key}\" (" \
          "#{field_names index.all_fields, true}, \"value_hash\" #{:text.to_s}, " \
          "PRIMARY KEY((#{field_names index.hash_fields})"

        cluster_key = index.order_fields
        ddl += ", #{field_names cluster_key}" unless cluster_key.empty?
        ddl += ", value_hash"
        ddl += '));'

        ddl
      end

      def initialize_client
        @cluster = Cassandra.cluster hosts: @hosts, port: @port,
                                     timeout: nil
        @client = @cluster.connect @keyspace
      end

      private

      def query_index_limit_for_sample(query, limit)
        query_tries = 0
        begin
          rows = []
          result = @client.execute(query, page_size: limit * 100 * 3)
          loop do
            tmp_rows = CassandraBackend.remove_any_null_place_holder_row(result.to_a)
            rows = CassandraBackend.remove_any_null_row(tmp_rows)

            break if result.last_page? or rows.size > limit * 100
            result = result.next_page
          end
        rescue
          STDERR.puts "query error detected for sampling #{query.to_s}"
          sleep 30
          all_retries = 10
          if query_tries < all_retries
            query_tries += 1
            retry
          end
        end
        rows.sample(limit)
      end

      def query_index(query)
        query_tries = 0
        begin
          rows = []
          result = @client.execute(query, page_size: 20_000)
          loop do
            rows += CassandraBackend.remove_all_null_place_holder_row(result.to_a)

            break if result.last_page?
            STDERR.puts "queried #{rows.size} rows: #{Time.now}"
            result = result.next_page
          end
        rescue Exception => e
          all_retries = 10
          STDERR.puts "query error detected for fetching all record on #{query.to_s}: attempt #{query_tries} / #{all_retries}"
          sleep 30
          if query_tries < all_retries
            query_tries += 1
            retry
          end
          throw e
        end
        rows
      end

      def add_value_hash(index, results)
        results.each do |r|
          extra_str = index.extra.sort_by { |e| e.id}.map{|e| r[e.id]}.join(',')
          r["value_hash"] = Zlib.crc32(extra_str)
        end
        results
      end

      def format_result(index, results)
        results = add_value_hash index, results
        fields = index.all_fields.to_a
        fail 'all record has empty field' if CassandraBackend.remove_all_null_place_holder_row(results).empty?
        Parallel.map(results, in_processes: 10) do |r|
          csv_row = index_row(r, fields)
          csv_row << r["value_hash"]
          csv_row = csv_row.map{|s| s.is_a?(String) ? s.dump : s} # escape special characters like newline
          csv_row
        end
      end

      def convert_id_2_uuid(value)
        case value
        when Numeric
          Cassandra::Uuid.new value.to_i
        when String
          Cassandra::Uuid.new value
        when nil
          #Cassandra::Uuid::Generator.new.uuid
          nil
        else
          value
        end
      end

      # Get a comma-separated list of field names with optional types
      # @return [String]
      def field_names(fields, types = false)
        fields.map do |field|
          name = "\"#{field.id}\""
          name += ' ' + cassandra_type(field.class).to_s if types
          name
        end.join ', '
      end

      # Get a Cassandra client, connecting if not done already
      def client
        return @client unless @client.nil?
        initialize_client
      end

      # Return the datatype to use in Cassandra for a given field
      # @return [Symbol]
      def cassandra_type(field_class)
        case [field_class]
        when [Fields::IntegerField]
          :int
        when [Fields::FloatField]
          :float
        when [Fields::StringField]
          :text
        when [Fields::DateField]
          :timestamp
        when [Fields::IDField],
            [Fields::ForeignKeyField], [Fields::CompositeKeyField]
          :uuid
        end
      end

      # Insert data into an index on the backend
      class InsertStatementStep < Backend::InsertStatementStep
        def initialize(client, index, fields)
          super

          @fields = fields.map(&:id) & index.all_fields.map(&:id)
          unless (index.key_fields.map(&:id).to_set - @fields.to_set).empty?
            STDERR.puts "some keys are missing for insert #{(index.key_fields).map(&:id).to_set - @fields.to_set}. added index.key_fields"
            @fields |= index.key_fields.map(&:id)
          end

          begin
            @prepared = client.prepare insert_cql
          rescue Exception => e
            puts e
            puts insert_cql
            throw e
          end
          @generator = Cassandra::Uuid::Generator.new
        end

        # Insert each row into the index
        def process(results)
          results.each do |result|
            fields_in_result = @index.all_fields.select { |field| result.key? field.id }
            unless (@index.key_fields - fields_in_result).empty?
              fields_in_result += @index.key_fields.to_a
              fields_in_result.uniq!
            end

            # sort fields according to the Insert
            fields_in_result.sort_by!{|field| @prepared.cql.index(field.id)} if @prepared.is_a? Cassandra::Statements::Prepared

            values = fields_in_result.map do |field|
              # if the result does not have the field, replace the field with place holder
              # This is required since the upsert modified cfs if any entity is overlapped
              next CassandraBackend.convert_nil_2_place_holder field unless result.has_key? field.id

              value = result[field.id]
              # If this is an ID, generate or construct a UUID object
              if field.is_a?(Fields::IDField)
                value = if value.nil?
                          @generator.uuid
                        else
                          Cassandra::Uuid.new(value.to_i)
                        end
              end

              # XXX Useful to test that we never insert null values
              # fail if value.nil?

              value = value.to_f if value.instance_of?(BigDecimal)
              value
            end

            extra_str = @index.extra.sort_by { |e| e.id}.map{|e| result[e.id]}.join(',')
            values << Zlib.crc32(extra_str).to_s

            begin
              @client.execute(@prepared, arguments: values)
            rescue ArgumentError => e
              STDERR.puts e.inspect
              STDERR.puts "Possible cause for this problem is too small number of "  +
                              "records in mysql and some support query gets empty result. target index was #{@index.key} : #{@index.hash_str}"
              throw e
            rescue Cassandra::Errors::InvalidError
              # We hit a value which does not actually need to be
              # inserted based on the data since some foreign
              # key in the graph corresponding to this column
              # family does not exist
              nil
            end
          end
        end

        # The CQL used to insert the fields into the index
        def insert_cql
          insert = "INSERT INTO #{@index.key} ("
          insert += (@fields.map { |f| "\"#{f}\"" }.join(', ') + ", value_hash ")
          insert << ') VALUES (' << (['?'] * (@fields.length + 1)).join(', ') + ')'

          insert
        end
      end

      private

      # Delete data from an index on the backend
      class DeleteStatementStep < Backend::DeleteStatementStep
        def initialize(client, index)
          super

          @index_keys = @index.hash_fields + @index.order_fields.to_set

          # Prepare the statement required to perform the deletion
          delete = "DELETE FROM #{index.key} WHERE "
          delete += @index_keys.map { |key| "\"#{key.id}\" = ?" }.join(' AND ')
          @prepared = client.prepare delete
        end

        # Execute the delete for a given set of keys
        def process(results)
          # Delete each row from the index
          results.each do |result|
            values = delete_values result
            @client.execute(@prepared, arguments: values)
          end
        end

        private

        # Get the values used in the WHERE clause for a CQL DELETE
        def delete_values(result)
          @index_keys.map do |key|
            cur_field = @index.all_fields.find { |field| field.id == key.id }

            if cur_field.is_a?(Fields::IDField)
              Cassandra::Uuid.new(result[key.id].to_i)
            else
              result[key.id]
            end
          end
        end
      end

      # A query step to look up data from a particular column family
      class IndexLookupStatementStep < Backend::IndexLookupStatementStep
        # rubocop:disable Metrics/ParameterLists
        def initialize(client, select, conditions, step, next_step, prev_step, later_indexlookup_steps = [])
          super(client, select, conditions, step, next_step, prev_step)

          @later_indexlookup_steps = later_indexlookup_steps

          @logger = Logging.logger['nose::backend::cassandra::indexlookupstep']

          # TODO: Check if we can apply the next filter via ALLOW FILTERING
          cql = select_cql(select + conditions.values.map(&:field).to_set + step.index.groupby_fields, conditions)
          STDERR.puts "query prepared : #{cql}"
          begin
            @prepared = client.prepare cql
          rescue => e
            puts e
            puts cql
            puts step.index.hash_str
            throw e
          end
        end
        # rubocop:enable Metrics/ParameterLists

        # Perform a column family lookup in Cassandra
        def process(conditions, results, query_conditions)
          results = initial_results(conditions) if results.nil?
          condition_list = result_conditions conditions, results
          new_result = fetch_all_queries condition_list, results, query_conditions
          new_result = CassandraBackend.remove_any_null_place_holder_row(new_result)
          fail "no result given" if new_result.size == 0 and not @step.children.empty?

          # Limit the size of the results in case we fetched multiple keys
          new_result[0..(@step.limit.nil? ? -1 : @step.limit)]
        end

        private

        # Produce the select CQL statement for a provided set of fields
        # @return [String]
        def select_cql(select, conditions)
          select = expand_selected_fields select, @later_indexlookup_steps
          select_fields = fields_with_aggregations select
          cql = "SELECT #{select_fields} FROM " \
                "\"#{@step.index.key}\" WHERE #{cql_where_clause conditions}"
          cql += cql_group_by
          cql += cql_order_by

          # Add an optional limit
          cql += " LIMIT #{@step.limit}" unless @step.limit.nil?

          cql
        end

        # Produce a CQL where clause using the given conditions
        # @return [String]
        def cql_where_clause(conditions)
          # TODO: sort eq_fields to put GROUP BY fields first
          where = @eq_fields.map do |field|
            "\"#{field.id}\" = ?"
          end.join ' AND '
          unless @range_field.nil?
            # TODO: allow several range fields
            condition = conditions.each_value.find(&:range?)
            where << " AND \"#{condition.field.id}\" #{condition.operator} ?"
          end

          where
        end

        # Produce the CQL ORDER BY clause for this step
        # @return [String]
        def cql_order_by
          # TODO: CQL3 requires all clustered columns before the one actually
          #       ordered on also be specified
          #
          #       Example:
          #
          #         SELECT * FROM cf WHERE id=? AND col1=? ORDER by col1, col2
          return '' if @step.order_by.empty?
          order_by_fields = @step.order_by
          unless @step.index.order_fields.take(@step.order_by.size) == @step.order_by
            fail 'order by fields are not included in order_fields'  \
              unless @step.index.order_fields.to_set >= @step.order_by.to_set

            order_by_fields = @step.order_by.to_set + @step.eq_filter.to_set - @step.index.hash_fields.to_set
            order_by_fields = order_by_fields.to_a.sort_by { |obf| @step.index.order_fields.find_index(obf)}
            fail 'order by fields and eq_filter does not match order_fields' \
              unless @step.index.order_fields.take(order_by_fields.size) == order_by_fields
          end
          ' ORDER BY ' + fields_with_aggregations(order_by_fields)
        end

        def fields_with_aggregations(select)
          select = select.to_a
          count_fields = select.select{|f| @step.index.count_fields.include? f}
          sum_fields = select.select{|f| @step.index.sum_fields.include? f}
          avg_fields = select.select{|f| @step.index.avg_fields.include? f}
          max_fields = select.select{|f| @step.index.max_fields.include? f}
          non_aggregate_fields = select - count_fields - sum_fields - avg_fields - max_fields

          fields = []
          non_aggregate_fields.each { |f| fields.append Hash[f, Set.new(["\"#{f.id}\""])]} unless non_aggregate_fields.empty?
          count_fields.each{|f| fields.append Hash[f, Set.new(["count(\"#{f.id}\")"])]} unless count_fields.empty?
          sum_fields.each{|f| fields.append Hash[f, Set.new(["sum(\"#{f.id}\")"])]} unless sum_fields.empty?
          avg_fields.each{|f| fields.append Hash[f, Set.new(["avg(\"#{f.id}\")"])]} unless avg_fields.empty?
          max_fields.each{|f| fields.append Hash[f, Set.new(["max(\"#{f.id}\")"])]} unless max_fields.empty?

          # field should be ordered in the given order especially for ORDER BY clause
          fields = fields.inject({}){|l, r| l.merge(r) {|_, l_v, r_v| Set.new([l_v, r_v])}}
          fields.sort_by{|k, _| select.index k}.flat_map{|_, v| v.map(&:to_s).join(', ')}.join ', '
        end

        def cql_group_by
          return '' if @step.index.groupby_fields.empty?
          ' GROUP BY ' + @step.index
                              .groupby_fields
                              .sort_by { |f| @step.index.hash_str.index(f.id)}.map { |f| "\"#{f.id}\"" }
                              .join(', ')
        end

        # Lookup values from an index selecting the given
        # fields and filtering on the given conditions
        def fetch_all_queries(condition_list, results, query_conditions)
          new_result = []
          @logger.debug { "  #{@prepared.cql} * #{condition_list.size}" }

          # TODO: Chain enumerables of results instead
          # Limit the total number of queries as well as the query limit
          condition_list.zip(results).each do |condition_set, result|
            # Loop over all pages to fetch results
            values = lookup_values condition_set, query_conditions
            fetch_query_pages values, new_result, result

            # Don't continue with further queries
            break if !@step.limit.nil? && new_result.length >= @step.limit
          end
          @logger.debug "Total result size = #{new_result.size}"

          new_result
        end

        # Get the necessary pages of results for a given list of values
        def fetch_query_pages(values, new_result, result)
          new_results = @client.execute(@prepared, arguments: values)
          loop do
            # Add the previous results to each row
            rows = new_results.map { |row| result.merge row }

            # XXX Ignore null values in results for now
            # fail if rows.any? { |row| row.values.any?(&:nil?) }

            new_result.concat rows
            break if new_results.last_page? ||
                (!@step.limit.nil? && result.length >= @step.limit)
            new_results = new_results.next_page
            @logger.debug "Fetched #{result.length} results"
          end
        end

        # Produce the values used for lookup on a given set of conditions
        def lookup_values(condition_set,query_conditions)
          condition_set.map do |condition|
            begin
              value = condition.value ||
                  query_conditions[condition.field.id].value
              fail "condition not found for #{condition.field.id}" if value.nil?
            rescue => e
              puts e
              puts condition.field.id
              throw e
            end
            if condition.field.is_a?(Fields::IDField)
              Cassandra::Uuid.new(value.to_i)
            else
              value
            end
          end
        end
      end
    end
  end
end
