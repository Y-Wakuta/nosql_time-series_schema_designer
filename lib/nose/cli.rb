require 'formatador'
require 'parallel'
require 'thor'
require 'yaml'

module NoSE
  # CLI tools for running the advisor
  module CLI
    # A command-line interface to running the advisor tool
    class NoSECLI < Thor
      check_unknown_options!

      class_option :debug, type: :boolean, aliases: '-d'
      class_option :parallel, type: :boolean, default: true
      class_option :colour, type: :boolean, default: nil, aliases: '-c'

      def initialize(_options, _local_options, config)
        super

        # Set up a logger for this command
        cmd_name = config[:current_command].name
        @logger = Logging.logger["nose::#{cmd_name}"]

        # Enable forcing the colour or no colour for output
        # We just lie to Formatador about whether or not $stdout is a tty
        unless options[:colour].nil?
          stdout_metaclass = class << $stdout; self; end
          method = options[:colour] ? ->() { true } : ->() { false }
          stdout_metaclass.send(:define_method, :tty?, &method)
        end

        # Disable parallel processing if desired
        Parallel.instance_variable_set(:@processor_count, 0) \
          unless options[:parallel]
      end

      private

      # Find the workload with the given name
      def get_workload(name)
        filename = File.expand_path "../../../workloads/#{name}.rb", __FILE__
        contents = File.read(filename)
        binding.eval contents, filename
      end

      # Load the configuration to use for a backend
      def load_config
        config = YAML.load_file File.join(Dir.pwd, 'nose.yml')
        config.deep_symbolize_keys
      end

      # Get a backend instance for a given configuration and dataset
      def get_backend(config, result)
        be_class = get_class 'backend', config
        be_class.new result.workload, result.indexes, result.plans,
                     config[:backend]
      end

      # Get a class of a particular name from the configuration
      def get_class(class_name, config)
        name = config
        name = config[class_name.to_sym][:name] if config.is_a? Hash
        require_relative "#{class_name}/#{name}"
        name = name.split('_').map(&:capitalize).join
        full_class_name = ['NoSE', class_name.capitalize,
                           name + class_name.capitalize]
        full_class_name.reduce(Object) do |mod, name_part|
          mod.const_get name_part
        end
      end

      # Collect all advisor results for schema design problem
      def search_result(workload, cost_model, max_space = Float::INFINITY)
        enumerated_indexes = IndexEnumerator.new(workload) \
          .indexes_for_workload.to_a
        Search::Search.new(workload, cost_model) \
          .search_overlap enumerated_indexes, max_space
      end

      # Load results of a previous search operation
      def load_results(plan_file)
        representer = Serialize::SearchResultRepresenter.represent \
          OpenStruct.new
        json = File.read(plan_file)
        representer.from_json(json)
      end

      # Output a list of indexes as text
      def output_indexes_txt(header, indexes, file)
        file.puts Formatador.parse("[blue]#{header}[/]")
        indexes.each { |index| file.puts index.inspect }
        file.puts
      end

      # Output a list of query plans as text
      def output_plans_txt(plans, file)
        plans.each do |plan|
          file.puts plan.query.inspect
          plan.each { |step| file.puts '  ' + step.inspect }
          file.puts
        end
      end

      # Output the results of advising as text
      def output_txt(result, file = $stdout, enumerated = false)
        if enumerated
          header = "Enumerated indexes\n" + '━' * 50
          output_indexes_txt header, result.enumerated_indexes, file
        end

        # Output selected indexes
        header = "Indexes\n" + '━' * 50
        output_indexes_txt header, result.indexes, file

        file.puts Formatador.parse("  Total size: " \
                                   "[blue]#{result.total_size}[/]\n\n")

        # Output query plans for the discovered indices
        header = "Query plans\n" + '━' * 50
        file.puts Formatador.parse("[blue]#{header}[/]")
        output_plans_txt result.plans, file

        unless result.update_plans.empty?
          header = "Update plans\n" + '━' * 50
          file.puts Formatador.parse("[blue]#{header}[/]")
        end

        result.update_plans.each do |statement, plans|
          file.puts statement.inspect
          plans.each do |plan|
            file.puts " for #{plan.index.key}"
            output_plans_txt plan.query_plans, file

            plan.update_steps.each do |step|
              file.puts '  ' + step.inspect
            end

            file.puts
          end

          file.puts "\n"
        end

        file.puts Formatador.parse('  Total cost: ' \
                                   "[blue]#{result.total_cost}[/]\n")
      end

      # Output the results of advising as JSON
      def output_json(result, file = $stdout, enumerated = false)
        # Temporarily remove the enumerated indexes
        if enumerated
          enumerated = result.enumerated_indexes
          result.delete_field :enumerated_indexes
        end

        file.puts JSON.pretty_generate \
          Serialize::SearchResultRepresenter.represent(result).to_hash

        result.enumerated_indexes = enumerated if enumerated
      end

      # Output the results of advising as YAML
      def output_yml(result, file = $stdout, enumerated = false)
        # Temporarily remove the enumerated indexes
        if enumerated
          enumerated = result.enumerated_indexes
          result.delete_field :enumerated_indexes
        end

        file.puts Serialize::SearchResultRepresenter.represent(result).to_yaml

        result.enumerated_indexes = enumerated if enumerated
      end

      # Filter an options hash for those only relevant to a given command
      def filter_command_options(opts, command)
        Thor::CoreExt::HashWithIndifferentAccess.new(opts.select do |key|
          self.class.commands[command].options.keys.map(&:to_sym).include? \
            key.to_sym
        end)
      end
    end
  end
end

# Require the various subcommands
require_relative 'cli/benchmark'
require_relative 'cli/create'
require_relative 'cli/load'
require_relative 'cli/genworkload'
require_relative 'cli/graph'
require_relative 'cli/proxy'
require_relative 'cli/reformat'
require_relative 'cli/repl'
require_relative 'cli/recost'
require_relative 'cli/search'
require_relative 'cli/search_all'
require_relative 'cli/search_bench'

# Only include the console command if pry is available
begin
  require 'pry'
  require_relative 'cli/console'
rescue LoadError
end
