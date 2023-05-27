defmodule Queue do
  use GenServer

  def start_link(user_name) do
    GenServer.start_link(__MODULE__, user_name, name: get_name(user_name))
  end

  def child_spec(user_name) do
    %{
      id: get_name(user_name),
      start: {Queue, :start_link, [user_name]}
    }
  end

  @impl true
  def init(user_name) do
    %{topics: topics} = read_filesystem_state(user_name)
    {:ok, %{user_name: user_name, topics: topics, sockets: []}}
  end

  @impl true
  def handle_call({:subscribe, topic}, _from, state) do
    add_topic_filesystem_state(state.user_name, topic)
    new_topics = state.topics |> MapSet.put(topic)
    {:reply, :ok, state |> Map.put(:topics, new_topics)}
  end

  @impl true
  def handle_call({:get_topics}, _from, state) do
    {:reply, state.topics |> Enum.to_list(), state}
  end

  @impl true
  def handle_call({:unsubscribe, topic}, _from, state) do
    remove_topic_filesystem_state(state.user_name, topic)
    new_topics = state.topics |> MapSet.delete(topic)
    {:reply, :ok, state |> Map.put(:topics, new_topics)}
  end

  @impl true
  def handle_call({:add_socket, socket}, _from, state) do
    sockets = state.sockets ++ [socket]
    %{messages: messages} = read_filesystem_state(state.user_name)

    messages =
      messages
      |> Enum.filter(fn message ->
        not message.acknowledged
      end)

    messages
    |> Enum.map(fn message ->
      send_message_all_sockets(message, [socket])
    end)

    {:reply, :ok, state |> Map.put(:sockets, sockets)}
  end

  @impl true
  def handle_call({:acknowledge, guid}, _from, state) do
    acknowledge_message_filesystem_state(state.user_name, guid)
    {:reply, :ok, state}
  end

  @impl true
  def handle_cast({:message, {topic, message}}, state) do
    message = %{topic: topic, message: message, guid: UUID.uuid4(:hex), acknowledged: false}

    case state.topics |> Enum.member?(topic) do
      true ->
        add_message(state.user_name, message)

        sockets =
          message
          |> send_message_all_sockets(state.sockets)

        {:noreply, state |> Map.put(:sockets, sockets)}

      false ->
        {:noreply, state}
    end
  end

  defp send_message_all_sockets(%{topic: topic, message: message, guid: guid}, sockets) do
    sockets
    |> Enum.reduce([], fn socket, acc ->
      list =
        case :gen_tcp.send(
               socket,
               "\r\nTopic: #{topic}\nMessage: #{message}\nGUID: #{guid}\n\r"
             ) do
          :ok -> [socket]
          {:error, _} -> []
        end

      acc ++ list
    end)
  end

  defp read_filesystem_state(user_name) do
    file_name = "#{get_name(user_name)}.bin"

    %{topics: topics, messages: messages} =
      case File.read(file_name) do
        {:ok, bytes} ->
          bytes |> :erlang.binary_to_term()

        {:error, :enoent} ->
          empty_state = %{topics: MapSet.new(), messages: []}
          write_filesystem_state(user_name, empty_state)
          empty_state
      end

    %{topics: topics, messages: messages}
  end

  defp write_filesystem_state(user_name, %{topics: topics, messages: messages}) do
    file_name = "#{get_name(user_name)}.bin"

    bytes = %{topics: topics, messages: messages} |> :erlang.term_to_binary()

    :ok = File.write(file_name, bytes)
  end

  defp add_topic_filesystem_state(user_name, topic) do
    %{topics: topics, messages: messages} = read_filesystem_state(user_name)
    new_topics = topics |> MapSet.put(topic)
    write_filesystem_state(user_name, %{topics: new_topics, messages: messages})
  end

  defp remove_topic_filesystem_state(user_name, topic) do
    %{topics: topics, messages: messages} = read_filesystem_state(user_name)
    new_topics = topics |> MapSet.delete(topic)
    write_filesystem_state(user_name, %{topics: new_topics, messages: messages})
  end

  defp add_message(user_name, message) do
    %{topics: topics, messages: messages} = read_filesystem_state(user_name)
    messages = messages ++ [message]
    write_filesystem_state(user_name, %{topics: topics, messages: messages})
  end

  defp acknowledge_message_filesystem_state(user_name, guid) do
    %{topics: topics, messages: messages} = read_filesystem_state(user_name)

    messages =
      messages
      |> Enum.filter(fn message ->
        message.guid != guid
      end)

    write_filesystem_state(user_name, %{topics: topics, messages: messages})
  end

  def subscribe(server, topic) do
    :ok = GenServer.call(server, {:subscribe, topic})
  end

  def get_topics(server) do
    GenServer.call(server, {:get_topics})
  end

  def unsubscribe(server, topic) do
    :ok = GenServer.call(server, {:unsubscribe, topic})
  end

  def add_socket(server, socket) do
    :ok = GenServer.call(server, {:add_socket, socket})
  end

  def acknowledge_message(server, guid) do
    :ok = GenServer.call(server, {:acknowledge, guid})
  end

  def send_message(server, topic, message) do
    GenServer.cast(server, {:message, {topic, message}})
  end

  def get_name(user_name) do
    :"#{__MODULE__}#{String.capitalize(user_name)}"
  end
end
