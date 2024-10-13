defmodule SimulIngestion.Indexing do
  alias SimulIngestion.Dashboard
  alias SimulIngestion.Pipeline
  use GenStage

  def start_link(args) do
    GenStage.start_link(__MODULE__, args, name: __MODULE__)
  end

  def init(args) do
    {:consumer, args, subscribe_to: get_subscribers(Keyword.fetch!(args, :embedding_nb))}
  end

  def handle_events(events, _from, state) do
    Dashboard.indexing_chunks(events, 0)
    {:noreply, [], state}
  end

  defp get_subscribers(nb_embeddings) do
    Enum.map(1..nb_embeddings, fn i ->
      {Pipeline.via("Embedding_#{i}"), max_demand: 60, min_demand: 45}
    end)
  end
end
