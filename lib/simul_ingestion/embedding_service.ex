defmodule SimulIngestion.EmbeddingService do
  alias SimulIngestion.Dashboard
  use GenServer
  # Note: use queue instead of list

  def start_link(args) do
    GenServer.start_link(__MODULE__, args, name: __MODULE__)
  end

  def embed(chunks) do
    GenServer.call(__MODULE__, {:embed, chunks}, :infinity)
  end

  @impl true
  def init(args) do
    state = %{
      max: Keyword.fetch!(args, :max_batch_per_min),
      cur: Keyword.fetch!(args, :max_batch_per_min),
      waiting: [],
      embedding_time_ms: Keyword.fetch!(args, :embedding_time_ms)
    }

    Process.send_after(self(), :tick, 1000)
    {:ok, state}
  end

  @impl true
  def handle_info(:tick, state) do
    # Ticking every second
    %{max: max, cur: cur, waiting: waiting, embedding_time_ms: embedding_time_ms} = state

    reply =
      cond do
        max / 60 + cur >= 1 and length(waiting) > 0 ->
          {chunks, from} = hd(waiting)
          # TODO: BUG it should process more than 1 event at a time
          # IO.puts("embedding #{length(chunks)} chunks")

          Task.start_link(fn ->
            Dashboard.embedding_chunks(chunks, embedding_time_ms)
            Process.sleep(embedding_time_ms)
            GenServer.reply(from, :ok)
          end)

          {:noreply, %{state | cur: cur - 1 + max / 60, waiting: tl(waiting)}}

        max / 60 + cur < max ->
          {:noreply, %{state | cur: cur + max / 60}}

        max == cur ->
          {:noreply, state}

        max / 60 + cur >= max ->
          IO.puts("Embedding service at max capacity")
          {:noreply, %{state | cur: max}}
      end

    Process.send_after(self(), :tick, 1_000)
    reply
  end

  @impl true
  def handle_call({:embed, chunks}, from, state) do
    %{cur: cur, waiting: waiting, embedding_time_ms: embedding_time_ms} = state

    if cur >= 1 do
      # IO.puts("embedding imm #{length(chunks)} chunks")

      Task.start_link(fn ->
        Dashboard.embedding_chunks(chunks, embedding_time_ms)
        Process.sleep(embedding_time_ms)
        GenServer.reply(from, :ok)
      end)

      {:noreply, %{state | cur: cur - 1}}
    else
      {:noreply, %{state | waiting: [{chunks, from} | waiting]}}
    end
  end
end
