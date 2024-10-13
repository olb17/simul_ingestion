defmodule SimulIngestion.Pipeline do
  use Supervisor

  def start_link(_args) do
    param = [
      chunking_nb: 5,
      embedding_nb: 100,
      max_batch_per_min: 400,
      embedding_time_ms: 300,
      chunking_per_sec: 6_000
    ]

    Supervisor.start_link(__MODULE__, param, name: __MODULE__)
  end

  @impl true
  def init(args) do
    chunking_nb = Keyword.fetch!(args, :chunking_nb)
    embedding_nb = Keyword.fetch!(args, :embedding_nb)
    max_batch_per_min = Keyword.fetch!(args, :max_batch_per_min)
    embedding_time_ms = Keyword.fetch!(args, :embedding_time_ms)
    chunking_per_sec = Keyword.fetch!(args, :chunking_per_sec)

    children =
      [
        {Registry, keys: :unique, name: Registry},
        SimulIngestion.Dashboard,
        {SimulIngestion.EmbeddingService,
         max_batch_per_min: max_batch_per_min, embedding_time_ms: embedding_time_ms},
        SimulIngestion.User
      ] ++
        chunking_children(chunking_nb, chunking_per_sec) ++
        embedding_children(embedding_nb, chunking_nb) ++
        [{SimulIngestion.Indexing, embedding_nb: embedding_nb}]

    Supervisor.init(children, strategy: :rest_for_one)
  end

  defp chunking_children(n, chunking_per_sec) do
    Enum.map(1..n, fn i ->
      Supervisor.child_spec(
        {SimulIngestion.Chunking, chunks_per_sec: chunking_per_sec, name: "Chunking_#{i}"},
        id: "Chunking_#{i}"
      )
    end)
  end

  defp embedding_children(n, chunking_nb) do
    Enum.map(1..n, fn i ->
      Supervisor.child_spec(
        {SimulIngestion.Embedding,
         batch_size: 15, chunking_nb: chunking_nb, name: "Embedding_#{i}"},
        id: "Embedding_#{i}"
      )
    end)
  end

  def via(name) do
    {:via, Registry, {Registry, name}}
  end
end
