
module NoSE
  module Migrator
    class Migrator

      def initialize(backend, loader)
        @backend = backend
        @loader = loader
      end

      # @param [MigratePlan, Backend]
      def prepare_next_indexes(migrate_plan, options)
        STDERR.puts "\e[36m migrate from: \e[0m"
        migrate_plan.obsolete_plan&.map{|step| STDERR.puts '  ' + step.inspect}
        STDERR.puts "\e[36m to: \e[0m"
        migrate_plan.new_plan.map{|step| STDERR.puts '  ' + step.inspect}

        migrate_plan.new_plan.steps.each do |new_step|
          next unless new_step.is_a? Plans::IndexLookupPlanStep
          query_plan = migrate_plan.prepare_plans.find{|pp| pp.index == new_step.index}&.query_plan
          next if query_plan.nil?

          target_index = new_step.index
          values = index_records(query_plan.indexes, target_index.all_fields)
          obsolete_data = full_outer_join(values)

          STDERR.puts "===== creating index: #{target_index.key} for the migration"
          unless @backend.index_exists?(target_index)
            STDERR.puts @backend.create_index(target_index, !options[:dry_run], options[:skip_existing])
          end
          STDERR.puts "collected data size for #{target_index.key} is #{obsolete_data.size}"
          @backend.load_index_by_COPY(target_index, obsolete_data)
          STDERR.puts "===== creation done: #{target_index.key} for the migration"

          MigrateValidator.new(@backend, @loader).validate(target_index, options) #if ENV['BENCH_MODE'] == 'debug'
        end
      end

      def exec_cleanup(result, timestep)
        STDERR.puts "cleanup"
        migration_plans = result.migrate_plans.select{|mp| mp.start_time == timestep}

        return if timestep + 1 == result.timesteps
        next_ts_indexes = result.time_depend_indexes.indexes_all_timestep[timestep + 1].indexes
        drop_obsolete_tables(migration_plans, next_ts_indexes)
      end

      private

      def index_records(indexes, required_fields)
        Hash[indexes.map do |index|
          values = @backend.index_records(index, required_fields).to_a
          [index, values]
        end]
      end

      def left_outer_join(left_index, left_values, right_index, right_values)
        overlap_fields = (left_index.all_fields & right_index.all_fields).to_a
        right_index_hash = {}

        starting = Time.now
        # create hash for right values
        right_values.each do |right_value|
          next if @backend.remove_null_place_holder_row([right_value]).empty?

          key_fields = overlap_fields.select{|f| f.is_a? NoSE::Fields::IDField}.map{|fi| right_value.slice(fi.id)}
          next if @backend.remove_null_place_holder_row(key_fields).empty?

          key_fields = overlap_fields.select{|f| f.is_a? NoSE::Fields::IDField}.map{|fi| right_value[fi.id].to_s}.join(',')
          key_fields = Zlib.crc32(key_fields)
          if right_index_hash.has_key?(key_fields)
            right_index_hash[key_fields] << right_value
          else
            right_index_hash[key_fields] = [right_value]
          end
        end
        puts "left outer join hash creation done: #{Time.now - starting}"

        results = []
        # iterate for left value to look for checking does related record exist
        left_values.each do |left_value|
          related_key = overlap_fields.select{|f| f.is_a? NoSE::Fields::IDField}.map{|fi| left_value[fi.id].to_s}.join(',')
          related_key = Zlib.crc32(related_key)
          if right_index_hash.has_key?(related_key)
            right_index_hash[related_key].each do |right_value|
              results << left_value.merge(right_value)
            end
          else
            results << left_value.merge(@backend.create_empty_record(right_index))
          end
        end.compact
        puts "hash join done #{Time.now - starting}"
        results
      end

      def full_outer_join(index_values)
        return index_values.to_a.flatten(1)[1] if index_values.length == 1

        result = []
        index_values.each_cons(2) do |former_index_value, next_index_value|
          puts "former index #{former_index_value[0].key} has #{former_index_value[1].size} records"
          puts "next index #{next_index_value[0].key} has #{next_index_value[1].size} records"
          result += left_outer_join(former_index_value[0], former_index_value[1], next_index_value[0], next_index_value[1])
          result += left_outer_join(next_index_value[0], next_index_value[1], former_index_value[0], former_index_value[1])
          result.uniq!
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
      def initialize(backend, loader)
        @backend = backend
        @loader = loader
        @logger = Logging.logger['nose::migrator::migratevalidator']
      end

      def validate(new_index, options)
        STDERR.puts "validating migration process for #{new_index.key}"
        load_dummy [new_index], options[:loader], options[:progress],
                   options[:limit], options[:skip_nonempty]
      end

      private

      def load_dummy(indexes, config, show_progress = false, limit = nil,
                     skip_existing = true)
        indexes.map!(&:to_id_graph).uniq! if @backend.by_id_graph

        # XXX Assuming backend is thread-safe
        indexes.each do |index|
          load_index_dummy index, config, show_progress, limit, skip_existing
        end
      end


      def load_index_dummy(index, config, show_progress, limit, skip_existing)
        @logger.info index.inspect if show_progress
        raw_results = @loader.query_for_index index, limit, config

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

              if left_values.first.class == Float
                if left_values.size > right_values.size
                  STDERR.puts "      left_values is larger than right_values : #{
                    left_values.sort.zip(right_values.sort).select{|l, r| (l - r).abs < 0.001}.map{|l, r| l}
                  }"
                else
                  STDERR.puts "      right_values is larger than left_values : #{
                    left_values.sort.zip(right_values.sort).select{|l, r| (l - r).abs < 0.001}.map{|l, r| r}
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
