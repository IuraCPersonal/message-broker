defmodule ConsumerConnectionAccepter do
  use Task, restart: :permanent
  require Logger

  def start_link(port) do
    Task.start_link(__MODULE__, :init, [port])
  end

  def init(port) do
    Process.register(self(), __MODULE__)
    Logger.info("#{__MODULE__} has started at pid #{self() |> inspect()}")

    # The options below mean:
    #
    # 1. `:binary` - receives data as binaries (instead of lists)
    # 2. `packet: :line` - receives data line by line
    # 3. `active: false` - blocks on `:gen_tcp.recv/2` until data is available
    # 4. `reuseaddr: true` - allows us to reuse the address if the listener crashes
    #
    {:ok, socket} = :gen_tcp.listen(port, [:binary, packet: :line, active: false, reuseaddr: true])
    Logger.info("#{__MODULE__} accepting connections on port #{port}")
    loop(socket)
  end

  def loop(socket) do
    {:ok, client} = :gen_tcp.accept(socket)
    Logger.info("New Consumer Connected")
    {:ok, pid} = Task.Supervisor.start_child(ConsumerConnectionTaskSupervisor, ConsumerConnectionHandler, :serve, [client])
    :ok = :gen_tcp.controlling_process(client, pid)
    loop(socket)
  end
end
