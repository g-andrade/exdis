defmodule Exdis.CommandParsers.Server do
  ## ------------------------------------------------------------------
  ## FLUSHDB Command
  ## ------------------------------------------------------------------

  def flush_db([]) do
    # TODO support async option
    exit(:todo)
  end

  def flush_db(_) do
    {:error, :bad_syntax}
  end
end
