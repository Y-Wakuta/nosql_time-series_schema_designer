
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

      def migrate(timestep)
        return unless timestep < @result.timesteps - 1
        migration_plans = @result.migrate_plans.select{|mp| mp.start_time == timestep}

        index_loaded_hash = {}
        get_under_constructing_indexes(timestep).each do |new_index|
          index_loaded_hash[new_index] = false
        end

        #Parallel.each(migration_plans, in_processes: Parallel.processor_count / 3) do |migration_plan|
        migration_plans.each do |migration_plan|
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

      def migrate_async(timestep)
        @worker = NoSE::Worker.new {|_| migrate(timestep)}
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

      def left_outer_join(left_index, left_values, right_index, right_values)
        overlap_key_fields = (left_index.all_fields & right_index.all_fields).select{|f| f.is_a? NoSE::Fields::IDField}
        puts "join on #{overlap_key_fields.inspect}"
        right_index_hash = {}

        starting = Time.now
        # create hash for right values
        right_values.each do |right_value|
          next if Backend::CassandraBackend.remove_all_null_place_holder_row([right_value]).empty?
          key_fields = overlap_key_fields.map{|fi| right_value.slice(fi.id)}
          next if Backend::CassandraBackend.remove_all_null_place_holder_row(key_fields).empty?

          key_fields = overlap_key_fields.map{|fi| right_value[fi.id].to_s}.join(',')
          key_fields = Zlib.crc32(key_fields)
          if right_index_hash[key_fields].nil?
            right_index_hash[key_fields] = [right_value]
          else
            right_index_hash[key_fields] << right_value
          end
        end

        results = Parallel.flat_map(left_values, in_threads: Parallel.processor_count / 3) do |left_value|
          tmp = []
          related_key = overlap_key_fields.map{|fi| left_value[fi.id].to_s}.join(',')
          related_key = Zlib.crc32(related_key)
          right_records = right_index_hash[related_key]
          if right_records.nil?
            tmp << join_with_empty_record(left_value, right_index)
          else
            right_records.each do |right_value|
              tmp << left_value.merge(right_value)
            end
          end
          tmp
        end.uniq

        puts "hash join done results #{results.size} records:  #{Time.now - starting}"
        results
      end

      def left_outer_join_groupby(left_index, left_values, right_index, right_values)
        overlap_key_fields = (left_index.all_fields & right_index.all_fields).select{|f| f.is_a? NoSE::Fields::IDField}

        starting = Time.now
        right_index_hash = right_values.reject{|rv| Backend::CassandraBackend.remove_all_null_place_holder_row([rv]).empty? \
              or Backend::CassandraBackend.remove_all_null_place_holder_row(overlap_key_fields.map{|fi| rv.slice(fi.id)}).empty?}.group_by do |right_value|
          (overlap_key_fields.map{|fi| right_value[fi.id].to_s}.join(',')).hash
        end
        right_index_hash.default = Backend::CassandraBackend.create_empty_record(right_index)

        results = left_values.flat_map do |left_value|
          related_key = (overlap_key_fields.map{|fi| left_value[fi.id].to_s}.join(',')).hash
          [right_index_hash[related_key]].flatten.map {|rv| rv.merge(left_value)}
        end.uniq

        puts "  new impl ruby original: hash join done results #{results.size} records:  #{Time.now - starting}"
        results
      end

      def join_with_empty_record(value, empty_record_index)
        Backend::CassandraBackend.create_empty_record(empty_record_index).merge(value)
      end

      def full_outer_join(index_values)
        return index_values.to_a.flatten(1)[1] if index_values.length == 1

        #result = []
        result_groupby = []
        index_values.each_cons(2) do |(former_index, former_value), (next_index, next_value)|
          puts "former index #{former_index.key} has #{former_value.size} records"
          puts "former index #{former_index.hash_str}"
          puts "next index #{next_index.key} has #{next_value.size} records"
          puts "next index #{next_index.hash_str}"
          puts "start join #{Time.now}"
          #result += left_outer_join(former_index, former_value, next_index, next_value)
          #result += left_outer_join(next_index, next_value, former_index, former_value)
          #result.uniq!
          #puts "full outer join done #{result.size} records by #{Time.now}"

          result_groupby += left_outer_join_groupby(former_index, former_value, next_index, next_value)
          result_groupby += left_outer_join_groupby(next_index, next_value, former_index, former_value)
          result_groupby.uniq!

          #if result.to_set != result_groupby.to_set
          #  result
          #  result_groupby
          #  fail "merged result was not same"
          #end
          puts "full outer join done with new impl #{result_groupby.size} records by #{Time.now}"
        end
        result_groupby
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
        compare_two_results(index, results_on_mysql, results_on_cassandra)
      end

      private

      def data_on_mysql(index)
        raw_results = @loader.query_for_index_full_outer_join index, nil, @loader_config
        #raw_results = @loader.query_for_index_inner_join index, nil, @loader_config

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
        results_on_mysql
      end

      def data_on_cassandra(index)
        #results_on_backend = @backend.unload_index_by_cassandra_unloader(index)
        results_on_backend = @backend.index_records(index, index.all_fields)
        results_on_backend.each {|r| r.delete('value_hash')}
        results_on_backend
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

              if left_values.first.class == Float
                if left_values.size > right_values.size
                  STDERR.puts "      left_values is larger than right_values : #{
                    left_values.sort.zip(right_values.sort).select{|l, r| (l - r).abs < 0.001}.map{|l, r| l}.take(100)
                  }"
                else
                  STDERR.puts "      right_values is larger than left_values : #{
                    left_values.sort.zip(right_values.sort).select{|l, r| (l - r).abs < 0.001}.map{|l, r| r}.take(100)
                  }"
                end
              else
                if left_values.size > right_values.size
                  STDERR.puts "      left_values is larger than right_values : #{left_values.difference(right_values).map(&:to_s)}"
                else
                  STDERR.puts "      right_values is larger than left_values : #{right_values.difference(left_values).map(&:to_s)}"
                end
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
    end
  end
end
