defmodule Exdis.CommandParsers.Transaction do
  ## ------------------------------------------------------------------
  ## DISCARD Command
  ## ------------------------------------------------------------------

  def discard([]) do
    {:ok, :discard_transaction}
  end

  def discard(_) do
    {:error, :bad_syntax}
  end

  ## ------------------------------------------------------------------
  ## EXEC Command
  ## ------------------------------------------------------------------

  def exec([]) do
    {:ok, :commit_transaction}
  end

  def exec(_) do
    {:error, :bad_syntax}
  end

  ## ------------------------------------------------------------------
  ## MULTI Command
  ## ------------------------------------------------------------------

  def multi([]) do
    {:ok, :start_transaction}
  end

  def multi(_) do
    {:error, :bad_syntax}
  end
end
