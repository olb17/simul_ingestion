defmodule SimulIngestion.Embedding do
  alias SimulIngestion.Pipeline
  alias SimulIngestion.EmbeddingService
  use GenStage

  def start_link(args) do
    name = Keyword.fetch!(args, :name)
    GenStage.start_link(__MODULE__, args, name: Pipeline.via(name))
  end

  def init(args) do
    batch_size = Keyword.fetch!(args, :batch_size)
    chunking_nb = Keyword.fetch!(args, :chunking_nb)
    state = nil

    {:producer_consumer, state, subscribe_to: get_subscribers(chunking_nb, batch_size)}
  end

  defp get_subscribers(chunking_nb, batch_size) do
    1..chunking_nb
    |> Enum.map(fn i ->
      {Pipeline.via("Chunking_#{i}"), min_demand: 0, max_demand: batch_size}
    end)
  end

  def handle_events(events, _from, state) do
    EmbeddingService.embed(events)
    {:noreply, events, state}
  end
end
