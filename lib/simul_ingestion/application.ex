defmodule SimulIngestion.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    experiment_param = [
      chunking_nb: 2,
      embedding_nb: 1,
      max_batch_per_min: 400,
      embedding_time_ms: 300,
      chunking_per_sec: 6_000
    ]

    children = [
      # {SimulIngestion.Pipeline, experiment_param}
    ]

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: SimulIngestion.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
