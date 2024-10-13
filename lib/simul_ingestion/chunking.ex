defmodule SimulIngestion.Chunking do
  alias SimulIngestion.Dashboard
  alias SimulIngestion.Pipeline
  use GenStage

  def start_link(args) do
    name = Keyword.fetch!(args, :name)
    GenStage.start_link(__MODULE__, args, name: Pipeline.via(name))
  end

  def init(args) do
    state = %{chunks_per_sec: Keyword.fetch!(args, :chunks_per_sec)}
    {:producer_consumer, state, subscribe_to: [{SimulIngestion.User, max_demand: 5}]}
  end

  def handle_events(events, _from, state) do
    chunks =
      events
      |> Enum.flat_map(fn {doc, nb_chunks} ->
        chunking_time = round(nb_chunks / state.chunks_per_sec * 1_000)
        Dashboard.start_chunking_book(doc, chunking_time)
        # NOTE: processing to do in //
        # IO.puts("chunking #{inspect(doc)} into #{nb_chunks} chunks")
        Process.sleep(chunking_time)
        1..nb_chunks |> Enum.map(&{doc, &1})
      end)

    {:noreply, chunks, state}
  end
end
