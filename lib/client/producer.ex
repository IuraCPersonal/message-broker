defmodule Client.Producer do
  def start do
    {:ok, socket} = :gen_tcp.connect('127.0.0.1', 8080, [:binary, active: false, packet: :line])
    loop(socket)
  end

  def loop(socket) do
    topic = IO.gets("Enter the topic: ") |> String.trim() |> String.downcase()
    message = IO.gets("Enter the message: ") |> String.trim()
    packet_map = %{topic: topic, message: message}
    packet = "#{packet_map |> Poison.Encoder.encode(%{strict_keys: true})}\n"
    :ok = :gen_tcp.send(socket, packet)
    loop(socket)
  end
end
