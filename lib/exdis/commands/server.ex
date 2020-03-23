defmodule Exdis.Commands.Server do
  ## ------------------------------------------------------------------
  ## FLUSHDB Command
  ## ------------------------------------------------------------------

  def flush_db(database, []) do
    # TODO support async option
    Exdis.Database.KeyRegistry.for_each_key(database,
      fn key ->
        _ = Exdis.Database.Key.delete(database, key)
      end)
  end

  def flush_db(_, _) do
    {:error, :bad_syntax}
  end

  ## ------------------------------------------------------------------
  ## KEYS Command
  ## ------------------------------------------------------------------

end
