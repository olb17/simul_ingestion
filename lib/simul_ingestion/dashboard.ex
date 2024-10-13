defmodule SimulIngestion.Dashboard do
  alias SimulIngestion.Dashboard
  use GenServer

  def start_link(_args) do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  def add_book(book, nb_chunks) do
    GenServer.cast(__MODULE__, {:add_book, book, nb_chunks, DateTime.utc_now()})
  end

  def start_chunking_book(book, chunking_time) do
    GenServer.cast(__MODULE__, {:start_chunking_book, book, chunking_time, DateTime.utc_now()})
  end

  def embedding_chunks(chunks, embed_time) do
    GenServer.cast(__MODULE__, {:embedding_book, chunks, embed_time, DateTime.utc_now()})
  end

  def indexing_chunks(chunks, index_time) do
    GenServer.cast(__MODULE__, {:indexing_book, chunks, index_time, DateTime.utc_now()})
  end

  def get_state() do
    GenServer.call(__MODULE__, :get_state)
  end

  defstruct books: %{},
            chunking_speed: 0.0,
            chunking: %{},
            embedding_speed: 0.0,
            embedding: %{},
            indexing_speed: 0.0,
            indexing: %{}

  defmodule Book do
    defstruct [
      :name,
      :chunks,
      :start_time,
      :processing_time,
      :chunking_time,
      :embedding_time,
      :indexing_time,
      :chunking,
      embedding: [],
      indexing: []
    ]
  end

  @impl true
  def init(_arg) do
    {:ok, %Dashboard{}}
  end

  @impl true
  def handle_cast({:add_book, name, nb_chunks, time}, state) do
    IO.inspect("Adding book #{name}")
    b = %Book{name: name, chunks: nb_chunks, start_time: time}
    new_books = Map.put(state.books, name, b)
    {:noreply, %{state | books: new_books}}
  end

  def handle_cast({:start_chunking_book, name, chunking_time, time}, state) do
    IO.inspect("Starting chunking for book #{name}")
    b = Map.get(state.books, name)
    b = %Book{b | chunking: time, chunking_time: chunking_time}
    new_books = Map.put(state.books, name, b)
    {:noreply, %{state | books: new_books}}
  end

  def handle_cast({:embedding_book, chunks, embed_time, time}, state) do
    new_books =
      chunks
      |> Enum.reduce(%{}, fn {book_name, _chunk_id}, acc ->
        Map.put(acc, book_name, Map.get(acc, book_name, 0) + 1)
      end)
      |> Enum.reduce(state.books, fn {book_name, nb_chunks}, acc ->
        IO.inspect("Embedding chunks for book #{book_name}")
        book = Map.get(acc, book_name)
        embedding = [{time, nb_chunks, embed_time} | book.embedding]

        nb_embedded_chunks =
          Enum.reduce(embedding, 0, fn {_, nb_chunks, _}, acc -> acc + nb_chunks end)

        new_book =
          if nb_embedded_chunks == book.chunks do
            [{first_time, _, _} | _] = r_embedding = Enum.reverse(embedding)

            %Book{
              book
              | embedding: r_embedding,
                embedding_time: DateTime.diff(time, first_time, :millisecond) + embed_time
            }
          else
            %Book{book | embedding: embedding}
          end

        Map.put(acc, book_name, new_book)
      end)

    {:noreply, %{state | books: new_books}}
  end

  def handle_cast({:indexing_book, chunks, index_time, time}, state) do
    new_books =
      chunks
      |> Enum.reduce(%{}, fn {book_name, _chunk_id}, acc ->
        Map.put(acc, book_name, Map.get(acc, book_name, 0) + 1)
      end)
      |> Enum.reduce(state.books, fn {book_name, nb_chunks}, acc ->
        IO.inspect("Indexing chunks for book #{book_name}")
        book = Map.get(acc, book_name)
        indexing = [{time, nb_chunks, index_time} | book.indexing]

        nb_indexed_chunks =
          Enum.reduce(indexing, 0, fn {_, nb_chunks, _}, acc -> acc + nb_chunks end)

        new_book =
          if nb_indexed_chunks == book.chunks do
            [{first_time, _, _} | _] = r_indexing = Enum.reverse(indexing)

            %Book{
              book
              | indexing: r_indexing,
                indexing_time: DateTime.diff(time, first_time, :millisecond) + index_time,
                processing_time: DateTime.diff(time, book.start_time, :millisecond) + index_time
            }
          else
            %Book{book | indexing: indexing}
          end

        Map.put(acc, book_name, new_book)
      end)

    {:noreply, %{state | books: new_books}}
  end

  @impl true
  def handle_call(:get_state, _from, state) do
    freq =
      state.books
      |> Enum.map(fn {_book_name, book} -> book.processing_time end)
      |> Enum.frequencies()

    reply = %{freq: freq}
    {:reply, reply, state}
  end
end
