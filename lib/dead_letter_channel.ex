defmodule DeadLetterChannel do
  use GenServer

  def start_link(:ok) do
    GenServer.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  @impl true
  def init(:ok) do
    {:ok, nil}
  end

  @impl true
  def handle_cast({:unsendable_message, message}, state) do
    new_dead_letters = [
      %{
        timestamp: System.system_time(:second),
        message: message
      }
    ]

    file_name = "#{__MODULE__}.json"

    dead_letters_list =
      case File.read(file_name) do
        {:ok, contents} -> Poison.Parser.parse!(contents)
        {:error, :enoent} -> []
      end

    file_content =
      (dead_letters_list ++ new_dead_letters)
      |> Poison.Encoder.encode(%{pretty: true})

    :ok = File.write(file_name, file_content)
    {:noreply, state}
  end

  def send_message(message) do
    :ok = GenServer.cast(__MODULE__, {:unsendable_message, message})
  end
end
