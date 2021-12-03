
module NoSE
  module Migrator
    class Migrator

      def initialize(backend, loader, result, loader_config, does_validate)
        @backend = backend
        @loader = loader
        @result = result
        @loader_config = loader_config
        @validator = MigrateValidator.new(@backend, @loader, @loader_config)
        @does_validate = does_validate
        @worker = nil
        @thread = nil
      end

      def migrate(timestep, does_migrate_async = true)
        return unless timestep < @result.timesteps - 1
        migration_plans = @result.migrate_plans.select{|mp| mp.start_time == timestep}

        index_loaded_hash = {}
        get_under_constructing_indexes(timestep).each do |new_index|
          index_loaded_hash[new_index] = false
        end

        parallelism = does_migrate_async ?  Parallel.processor_count / 3 : 0
        Parallel.each(migration_plans, in_processes: parallelism) do |migration_plan|
          @backend.initialize_client
          prepare_next_indexes(migration_plan, index_loaded_hash)
        end
      end

      def create_next_indexes(timestep)
        get_under_constructing_indexes(timestep).uniq.each do |new_index|
          @backend.create_index(new_index, true, true)
          STDERR.puts new_index.key + " is created before query processing"
        end
      end

      def migrate_async(timestep, does_migrate_async)
        @worker = NoSE::Worker.new {|_| migrate(timestep, does_migrate_async)}
        [@worker].map(&:run).each(&:join)
        @thread = @worker.execute
        @thread.join
      end

      def stop
        @worker.stop
      end

      def get_under_constructing_indexes(timestep)
        return [] unless timestep < @result.timesteps - 1
        next_indexes = @result.time_depend_indexes.indexes_all_timestep[timestep + 1].indexes.to_set
        current_indexes = @result.time_depend_indexes.indexes_all_timestep[timestep].indexes.to_set
        next_indexes - current_indexes
      end

      # @param [MigratePlan, Backend]
      def prepare_next_indexes(migrate_plan, index_loaded_hash)
        STDERR.puts "\e[36m migrate from: \e[0m"
        migrate_plan.obsolete_plan&.map{|step| STDERR.puts '  ' + step.inspect}
        STDERR.puts "\e[36m to: \e[0m"
        migrate_plan.new_plan.map{|step| STDERR.puts '  ' + step.inspect}

        migrate_plan.new_plan.steps.each do |new_step|
          next unless new_step.is_a? Plans::IndexLookupPlanStep
          target_index = new_step.index
          query_plan = migrate_plan.prepare_plans.find{|pp| pp.index == target_index}&.query_plan
          next if query_plan.nil?

          unless index_loaded_hash[target_index]
            values = collect_records(query_plan.indexes)
            #values = index_records(query_plan.indexes)
            obsolete_data = full_outer_join(values)

            STDERR.puts "collected data size for #{target_index.key} is #{obsolete_data.size}: #{Time.now}"
            @backend.load_index_by_cassandra_loader(target_index, obsolete_data)
            STDERR.puts "===== creation done: #{target_index.key} for the migration"
            index_loaded_hash[target_index] = true
          end

          @validator.validate(target_index) if @does_validate
        end
      end

      def exec_cleanup(timestep)
        STDERR.puts "cleanup"
        migration_plans = @result.migrate_plans.select{|mp| mp.start_time == timestep}

        return if timestep + 1 == @result.timesteps
        next_ts_indexes = @result.time_depend_indexes.indexes_all_timestep[timestep + 1].indexes
        drop_obsolete_tables(migration_plans, next_ts_indexes)
      end

      private

      def index_records(indexes)
        Hash[indexes.map do |index|
          STDERR.puts "start collecting data from #{index.key} for the migration: #{Time.now}"
          values = @backend.index_records(index, index.all_fields).to_a
          [index, values]
        end]
      end

      def collect_records(indexes)
        Hash[indexes.map do |index|
          STDERR.puts "start collecting data from #{index.key} for the migration: #{Time.now}"
          values = @backend.unload_index_by_cassandra_unloader(index)
          [index, values]
        end]
      end

      def create_hash_table(index, values, key_fields)
        hash_table = values.reject{|rv| Backend::CassandraBackend.remove_all_null_place_holder_row([rv]).empty? \
             or Backend::CassandraBackend.remove_all_null_place_holder_row(key_fields.map{|fi| rv.slice(fi.id)}).empty?}.group_by do |right_value|
          (key_fields.map{|fi| right_value[fi.id].to_s}.join(',')).hash
        end
        hash_table.default = Backend::CassandraBackend.create_empty_record(index)
        hash_table
      end

      def left_outer_join(left_index, left_values, right_index, right_values)
        overlap_key_fields = (left_index.all_fields & right_index.all_fields).select{|f| f.is_a? NoSE::Fields::IDField}

        starting = Time.now
        right_index_hash = create_hash_table right_index, right_values, overlap_key_fields

        results = left_values.flat_map do |left_value|
          related_key = (overlap_key_fields.map{|fi| left_value[fi.id].to_s}.join(',')).hash
          [right_index_hash[related_key]].flatten.map {|rv| rv.merge(left_value)}
        end.uniq

        puts "hash join results #{results.size} records:  #{Time.now - starting}, #{left_index.key} <-> #{right_index.key}"
        results
      end

      def join_with_empty_record(value, empty_record_index)
        Backend::CassandraBackend.create_empty_record(empty_record_index).merge(value)
      end

      def full_outer_join(index_values)
        return index_values.to_a.flatten(1)[1] if index_values.length == 1

        result = []
        index_values.each_cons(2) do |(former_index, former_value), (next_index, next_value)|
          puts "former index #{former_index.key} has #{former_value.size} records"
          puts "  former index #{former_index.hash_str}"
          puts "next index #{next_index.key} has #{next_value.size} records"
          puts "  next index #{next_index.hash_str}"

          start_time = Time.now
          result += left_outer_join(former_index, former_value, next_index, next_value)
          result += left_outer_join(next_index, next_value, former_index, former_value)
          result.uniq!
          puts "full outer join done with new impl #{result.size} records by #{Time.now - start_time}"
        end
        result
      end

      def drop_obsolete_tables(migrate_plans, next_ts_indexes)
        obsolete_indexes = migrate_plans.flat_map do |mp|
          next if mp.obsolete_plan.nil?
          mp.obsolete_plan.indexes.select {|index| not next_ts_indexes.include?(index)}
        end.uniq
        obsolete_indexes.each do |obsolete_index|
          STDERR.puts "drop CF: #{obsolete_index.key}"
          @backend.drop_index(obsolete_index)
        end
      end
    end

    class MigrateValidator
      def initialize(backend, loader, loader_config)
        @backend = backend
        @loader = loader
        @loader_config = loader_config
      end

      def validate(index)
        STDERR.puts "validating migration process for #{index.key}"
        results_on_mysql = data_on_mysql index
        results_on_cassandra = data_on_cassandra index
        compare_two_records_in_rows(index,"mysql", results_on_mysql, "cassandra", results_on_cassandra)
        compare_two_results(index,"mysql", results_on_mysql, "cassandra", results_on_cassandra)
      end

      private

      def data_on_mysql(index)
        #raw_results = @loader.query_for_index_full_outer_join index, nil, @loader_config
        raw_results = @loader.query_for_index_inner_join index, nil, @loader_config

        results_on_mysql = raw_results.map do |row|
          row.each do |f, v|
            current = index.all_fields.find{|field| field.id == f}
            row[f] = @backend.index_row(row, [current]).first
          end
          row
        end
        load_and_unload index, results_on_mysql
      end

      def load_and_unload(index, records)
        index_tmp = index.dup
        index_tmp.key = "tmp_#{rand(0..100)}"
        @backend.create_index(index_tmp, true)
        @backend.load_index_by_cassandra_loader(index_tmp, records)
        @backend.unload_index_by_cassandra_unloader(index_tmp)
      end

      def data_on_cassandra(index)
        results_on_backend = @backend.unload_index_by_cassandra_unloader(index)
        results_on_backend.each {|r| r.delete('value_hash')}
        results_on_backend
      end

      def compare_two_records_in_rows(index, left_label, left_hash, right_label, right_hash)
        left_hash = @backend.cast_records index, left_hash
        right_hash = @backend.cast_records index, right_hash

        columns = left_hash.first.keys.sort
        left_str_rows = left_hash.map do |row_hash|
          columns.map {|c| c.class == Float ? row_hash[c].round(2) : row_hash[c]}.join(",")
        end.to_set
        right_str_rows = right_hash.map do |row_hash|
          columns.map {|c| row_hash[c]}.join(",")
        end.to_set

        if left_hash.size > right_hash.size
          puts "#{left_label} has more records than #{right_label}"
          puts left_str_rows - right_str_rows
        elsif right_hash.size > left_hash.size
          puts "#{right_label} has more records than #{left_label}"
          puts right_str_rows - left_str_rows
        else
          puts "#{right_label} and #{left_label} have the same number of record on #{index.key}"
          puts "#{right_label} - #{left_label}"
          puts right_str_rows - left_str_rows
          puts "#{left_label} - #{right_label}"
          puts left_str_rows - right_str_rows
        end
      end

      def compare_two_results(index, left_label, left_hash, right_label, right_hash)
        fail 'result field does not match' unless left_hash.first.keys.to_set == right_hash.first.keys.to_set
        left_hash = @backend.cast_records index, left_hash
        right_hash = @backend.cast_records index, right_hash

        Parallel.each(left_hash.first.keys, in_processes: Parallel.processor_count / 3) do |field_name|
          puts "start comparing #{field_name}: #{Time.now}"
          start_time = Time.now

          left_values = left_hash.map{|lh| lh[field_name]}
          right_values = right_hash.map{|rh| rh[field_name]}

          left_values = format_values left_values
          right_values = format_values right_values

          begin
            compare_field field_name, index.key, left_label, left_values, right_label, right_values
          rescue => e
            STDERR.puts "#{e}, #{field_name}, #{index.key}"
          end

          puts "comparing #{field_name} took: #{Time.now - start_time}"
        end
      end

      def compare_field(field_name, index_key, left_label, left_values, right_label, right_values)
           if left_values == right_values
            STDERR.puts "    #{field_name} in #{index_key} matches"
          else
            if compare_approximately_values left_values, right_values
              STDERR.puts "    === #{field_name} in #{index_key} approximately match #{left_values.size} <-> #{right_values.size}==="

              if left_values.first.class == Float
                 STDERR.puts "     value size is different for #{field_name} (left_values: #{left_values.size}, right_values: #{right_values.size}): #{
                    left_values.take(500).zip(right_values.take(500))
                               .select{|l, r| (l - r).abs > 0.001}.map{|l, _| l}.uniq.take(10)
                 }"
              else
                if left_values.size != right_values.size
                  STDERR.puts "     value size is different for #{field_name} (left_values: #{left_values.size}," \
                                " right_values: #{right_values.size}): #{
                    left_values.size > right_values.size ?
                      left_values.diff(right_values).uniq.map(&:to_s)
                      : right_values.diff(left_values).uniq.map(&:to_s)
                  }"
                end
              end
            else
              # this possibly happen if there are INSERT or UPDATE
              STDERR.puts "    === #{field_name} in #{index_key} does not match ==="
              STDERR.puts "     #{left_label} #{left_values.size}: #{left_values.take(10).map(&:to_s)}"
              STDERR.puts "     #{right_label} #{right_values.size}: #{right_values.take(10).map(&:to_s)}"
              STDERR.puts "    ==========================================="
            end
          end
      end

      def compare_approximately_values(left_values, right_values)
        return false if (left_values.size - right_values.size).abs > left_values.size * 0.01

        return false if left_values.diff(right_values).size > left_values.size * 0.01 \
                      or right_values.diff(left_values).size > right_values.size * 0.01
        true
      end

      def format_values(values)
        target_class = values.first.class
        if target_class == Cassandra::Uuid
          values.map!(&:to_s)
        elsif values.first.class == Float
          values.map!{|l| (l * 1_000).to_i}
        end
        values.sort!
        values
      end
    end
  end
end
