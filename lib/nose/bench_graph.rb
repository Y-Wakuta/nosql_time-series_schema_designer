require 'fileutils'
require "pycall/import"

include PyCall::Import
pyimport 'matplotlib.pyplot', as: "plt"
pyimport 'numpy', as: "np"

module NoSE
  module Graph
    class BenchGraph

      def initialize(result)
        @update_total = "UPDATE_TOTAL"
        @read_total = "READ_TOTAL"
        @total = "TOTAL"
        @bench_results = result.time_depend_plans.map{|tdp| tdp.plans[0].query}.map do |query|
          BenchResult.new query
        end
        @bench_results += result.workload.updates.map do |update|
          BenchResult.new update
        end
        @bench_results << BenchResult.new(@update_total)
        @bench_results << BenchResult.new(@read_total)
        @bench_results << BenchResult.new(@total)
      end

      def add_bench_result(statement, plan, execution_time)
        br = @bench_results.select{|br| br.statement == statement}.first
        br.add_execution_time(plan, execution_time)
      end

      def plot_results(label)
        plt = PyCall.import_module('matplotlib.pyplot')
        @bench_results.each do |br|
          plt = br.plot_result(plt, label)
        end
        plt.show()

      end

      class BenchResult
        attr_accessor :statement,:plans, :execution_times

        def initialize(statement)
          @statement = statement
          @plans = []
          @execution_times = []
        end

        def add_execution_time(plan, execution_time)
          @plans << plan
          @execution_times << execution_time
        end

        def plot_result(plt, label)
          STDERR.puts self.inspect
          @plans.each do |plan|
            STDERR.puts plan.inspect
          end

          plt.rcParams["xtick.direction"] = "in"
          #fig = plt.figure(figsize:[10,15], tight_layout: true)
          fig = plt.figure(tight_layout: true)
          fig.canvas.set_window_title(@statement.text)
          fig.subplots_adjust(wspace: 2, hspace: 2)
          ax1 = fig.add_subplot(211, title: @statement.comment, xlim:[0, @execution_times.size], ylim: [0, @execution_times.max])

          ax1.tick_params(axis: "both", which: "major", length: 4.5, width: 1.5)
          ax1.set_xlabel("timesteps", fontsize: 17, labelpad: 10)
          ax1.set_ylabel("latency", fontsize: 17, labelpad: 10)

          #ax1.plot((0...@execution_times.size).to_a, @execution_times.map{|mt| mt.mean})
          ax1.plot((0...@execution_times.size).to_a, @execution_times)
          dir = './graphs/' + label
          FileUtils.mkdir_p(dir)
          plt.savefig(dir + "/" + @statement.comment)
          plt
        end

        def inspect
          "- #{@statement.text}, \n "\
        "    -- #{@execution_times}"
          #"    -- #{@execution_times.map{|et| et.mean}}"
        end
      end
    end
  end
end
