defmodule Exdis.CommandParsers.Transaction do
  ## ------------------------------------------------------------------
  ## DISCARD Command
  ## ------------------------------------------------------------------

  def discard([]) do
    :discard_transaction
  end

  def discard(_) do
    {:error, :bad_syntax}
  end

  ## ------------------------------------------------------------------
  ## EXEC Command
  ## ------------------------------------------------------------------

  def exec([]) do
    :execute_transaction
  end

  def exec(_) do
    {:error, :bad_syntax}
  end

  ## ------------------------------------------------------------------
  ## MULTI Command
  ## ------------------------------------------------------------------

  def multi([]) do
    :start_transaction
  end

  def multi(_) do
    {:error, :bad_syntax}
  end
end
