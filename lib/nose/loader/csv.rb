require 'formatador'
require 'smarter_csv'
require 'zlib'

module NoSE::Loader
  # Load data into an index from a set of CSV files
  class CsvLoader < LoaderBase
    # Load data for all the indexes
    def load(indexes, config, show_progress = false)
      simple_indexes = indexes.select { |index| index.path.length == 1 }
      simple_indexes = simple_indexes.group_by { |index| index.path.first }
      simple_indexes.each do |entity, simple_index_list|
        filename = File.join config[:directory], "#{entity.name}.csv"
        total_rows = -1  # account for header row
        File.foreach(filename) { total_rows += 1 }

        if show_progress
          puts "Loading simple indexes for #{entity.name}"
          puts "#{simple_index_list.map(&:key).join ', '}"

          Formatador.new.redisplay_progressbar 0, total_rows
          progress = Formatador::ProgressBar.new total_rows,
                                                 started_at: Time.now
        else
          progress = nil
        end

        SmarterCSV.process(filename,
                           chunk_size: 1000,
                           convert_values_to_numeric: false) do |chunk|
          Parallel.each(chunk.each_slice(100),
                        finish: (lambda do |_, _, _|
                          inc = [progress.total - progress.current, 100].min
                          progress.increment inc if progress
                        end)) do |minichunk|
            load_simple_chunk minichunk, entity, simple_index_list
          end
        end
      end
    end

    private

    # Load a chunk of data from a simple entity index
    def load_simple_chunk(chunk, entity, indexes)

      # Prefix all hash keys with the entity name and convert values
      chunk.map! do |row|
        index_row = {}
        row.keys.each do |key|
          field_class = entity[key.to_s].class
          value = field_class.value_from_string row[key]
          index_row["#{entity.name}_#{key}"] = value
        end

        index_row
      end

      # Insert the batch into the index
      indexes.each do |index|
        @backend.index_insert_chunk index, chunk
      end
    end
  end
end