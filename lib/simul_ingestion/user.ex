defmodule SimulIngestion.User do
  alias SimulIngestion.Dashboard
  use GenStage

  def start_link(args) do
    GenStage.start_link(__MODULE__, args, name: __MODULE__)
  end

  def send_doc(doc, chunk_nb) do
    GenServer.call(__MODULE__, {:send_doc, doc, chunk_nb})
  end

  def send_small_doc(doc) do
    send_doc(doc, 65)
  end

  def send_big_doc(doc) do
    send_doc(doc, 200)
  end

  @impl true
  def init(_args) do
    {:producer, {:queue.new(), 0}}
  end

  @impl true
  def handle_call({:send_doc, doc, nb_chunks}, from, {queue, pending_demand}) do
    queue = :queue.in({doc, nb_chunks}, queue)
    GenStage.reply(from, :ok)
    Dashboard.add_book(doc, nb_chunks)
    dispatch_events(queue, pending_demand, [])
  end

  @impl true
  def handle_demand(incoming_demand, {queue, pending_demand}) do
    IO.puts("incoming demand : #{incoming_demand}")
    dispatch_events(queue, incoming_demand + pending_demand, [])
  end

  defp dispatch_events(queue, 0, events) do
    {:noreply, Enum.reverse(events), {queue, 0}}
  end

  defp dispatch_events(queue, demand, events) do
    case :queue.out(queue) do
      {{:value, event}, queue} ->
        # GenStage.reply(from, :ok)
        dispatch_events(queue, demand - 1, [event | events])

      {:empty, queue} ->
        {:noreply, Enum.reverse(events), {queue, demand}}
    end
  end
end
