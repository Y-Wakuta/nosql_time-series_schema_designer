# cited from https://qiita.com/yuroyoro/items/92c5bc864fa9c05127a9
module NoSE
  class Worker
    attr_reader :pid

    def initialize(&block)
      @child_read, @parent_write = create_pipe
      @parent_read, @child_write = create_pipe
      @block = block
    end

    def create_pipe
      # change the encoding to ASCII-8BIT
      IO.pipe.map{|pipe| pipe.tap{|_| _.set_encoding("ASCII-8BIT", "ASCII-8BIT")}}
    end

    # run the child process
    def run

      @pid = fork do
        @parent_read.close
        @parent_write.close

        # notify the start of child process
        write_to_parent(:ready)

        loop do
          # waiting for the request from the parent
          args = read_from_parent

          # exit the loop and kill this child process when get stop
          break if args == :stop

          # execute the task
          result = @block.call(*args)

          # write the result to the pipe and notice the end of task
          write_object(result, @child_write)
        end

        @child_read.close
        @child_write.write
      end

      wait_after_fork if @pid
    end

    def execute(*msg)
      write_to_child(msg)

      Thread.new { read_from_child }
    end

    def stop
      return unless alive?

      # stop the child
      write_to_child(:stop)

      # wait for child by waitpid
      Process.wait(@pid)
    end

    def write_object(obj, write)
      # write ruby object to pipe by using Marshal
      data = Marshal.dump(obj).gsub("\n", '\n') + "\n"
      write.write data
    end

    def read_object(read)
      # decode the object
      data = read.gets
      Marshal.load(data.chomp.gsub('\n', "\n"))
    end

    def write_to_child(obj)
      write_object(obj, @parent_write)
    end

    def read_from_child
      read_object(@parent_read)
    end

    def read_from_parent
      read_object(@child_read)
    end

    def write_to_parent(obj)
      write_object(obj, @child_write)
    end

    def wait_after_fork
      @child_read.close
      @child_write.close

      install_exit_handler
      install_signal_handler
    end

    def alive?
      Process.kill(0, @pid)
      true
    rescue Errno::ESRCH
      false
    end

    def install_exit_handler
      # exit the child by Kernel#at_exit
      at_exit do
        next unless alive?
        begin
          Process.kill("KILL", @pid)
          Process.wait(@pid)
        rescue Errno::ESRCH
          # do nothing
        rescue => e
          puts "error at_exit: #{e}"
          raise e
        end
      end
    end

    def install_signal_handler
      # send parent's SIGINT and SIGQUIT to child
      [:INT, :QUIT].each do |signal|
        old_handler = Signal.trap(signal) {
          Process.kill(signal, @pid)
          Process.wait(@pid)
          old_handler.call
        }
      end
    end
  end
end
