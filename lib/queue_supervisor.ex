defmodule QueueSupervisor do
  use Supervisor

  def start_link(init_arg \\ :ok) do
    Supervisor.start_link(__MODULE__, init_arg, name: __MODULE__)
  end

  @impl true
  def init(_init_arg) do
    children = recover_child_spec()

    Supervisor.init(children, strategy: :one_for_one)
  end

  defp recover_child_spec() do
    Path.wildcard("Elixir.Queue*.bin")
    |> Enum.map(fn x -> Regex.run(~r/Elixir\.Queue(.*)\.bin/, x) |> Enum.at(1) end)
    |> Enum.map(&String.downcase/1)
    |> Enum.map(&Queue.child_spec/1)
  end
end
