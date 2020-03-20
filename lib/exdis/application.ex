defmodule Exdis.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  def start(_type, _args) do
    # List all child processes to be supervised
    children = [
      Exdis.Regulator,
      Exdis.Database.KeyRegistry,
      Exdis.Listener
      # Starts a worker by calling: Exdis.Worker.start_link(arg)
      # {Exdis.Worker, arg},
    ]

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :rest_for_one, name: Exdis.Supervisor]
    Supervisor.start_link(children, opts)
  end
end
