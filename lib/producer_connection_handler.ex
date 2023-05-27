defmodule ProducerConnectionHandler do
  require Logger

  def serve(socket) do
    case :gen_tcp.recv(socket, 0) do
      {:ok, data} ->
        Logger.info("Producer Data Received")
        handle_message(data)
        serve(socket)

      {:error, :closed} ->
        Logger.info("Producer Connection Closed")
    end
  end

  defp handle_message(data) do
    case Poison.Parser.parse!(data, %{}) do
      %{"topic" => topic, "message" => message} when topic != "" and message != "" ->
        Supervisor.which_children(QueueSupervisor)
        |> Enum.map(fn {_, pid, _, _} ->
          Queue.send_message(pid, topic, message)
        end)

        Logger.info("#{__MODULE__} --- POST ---> Topic Queues")

      _ ->
        DeadLetterChannel.send_message(data)
        Logger.info("#{__MODULE__} --- POST ---> #{DeadLetterChannel}")
    end
  end
end
