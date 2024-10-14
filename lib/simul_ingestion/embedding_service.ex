defmodule SimulIngestion.EmbeddingService do
  alias SimulIngestion.Dashboard
  use GenServer
  # Note: use queue instead of list

  @ticker 1_000

  def start_link(args) do
    GenServer.start_link(__MODULE__, args, name: __MODULE__)
  end

  def embed(chunks) do
    GenServer.call(__MODULE__, {:embed, chunks}, :infinity)
  end

  @impl true
  def init(args) do
    max_batch_per_min = Keyword.fetch!(args, :max_batch_per_min) |> dbg
    embedding_time_ms = Keyword.fetch!(args, :embedding_time_ms) |> dbg

    state = %{
      max: max_batch_per_min,
      cur: max_batch_per_min,
      waiting: :queue.new(),
      embedding_time_ms: embedding_time_ms
    }

    Process.send_after(self(), :tick, @ticker)
    {:ok, state}
  end

  @impl true
  def handle_info(:tick, state) do
    # Ticking every @ticker
    %{max: max, cur: cur, waiting: waiting, embedding_time_ms: embedding_time_ms} = state

    new_cur = cur + max / 60 * @ticker / 1_000

    reply =
      cond do
        new_cur >= 1 and :queue.len(waiting) > 0 ->
          nb_tasks = Enum.min([floor(new_cur), :queue.len(waiting)])
          {processing, new_waiting} = :queue.split(nb_tasks, waiting)

          Enum.each(:queue.to_list(processing), fn {chunks, from} ->
            Task.start_link(fn ->
              Dashboard.embedding_chunks(chunks, embedding_time_ms)
              Process.sleep(embedding_time_ms)
              GenServer.reply(from, :ok)
            end)
          end)

          {:noreply, %{state | cur: cur - nb_tasks, waiting: new_waiting}}

        new_cur < max ->
          {:noreply, %{state | cur: new_cur}}

        max == cur ->
          {:noreply, state}

        new_cur > max ->
          IO.puts("Embedding service at max capacity")
          {:noreply, %{state | cur: max}}
      end

    Process.send_after(self(), :tick, @ticker)
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
      {:noreply, %{state | waiting: :queue.in({chunks, from}, waiting)}}
    end
  end
end
