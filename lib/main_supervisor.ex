defmodule MainSupervisor do
  use Supervisor

  def start_link(init_arg \\ :ok) do
    Supervisor.start_link(__MODULE__, init_arg, name: __MODULE__)
  end

  @impl true
  def init(_init_arg) do
    children = [
      {DeadLetterChannel, :ok},
      {QueueSupervisor, :ok},
      {Task.Supervisor, name: ConsumerConnectionTaskSupervisor},
      {ConsumerConnectionAccepter, 8081},
      {Task.Supervisor, name: ProducerConnectionTaskSupervisor},
      {ProducerConnectionAccepter, 8080}
    ]

    Supervisor.init(children, strategy: :rest_for_one)
  end
end
