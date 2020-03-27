defmodule Exdis.CommandParsers.Transaction do
  ## ------------------------------------------------------------------
  ## DISCARD Command
  ## ------------------------------------------------------------------

  def discard([]) do
    :discard_transaction
  end

  def discard(_) do
    {:error, {:wrong_number_of_arguments, :"DISCARD"}}
  end

  ## ------------------------------------------------------------------
  ## EXEC Command
  ## ------------------------------------------------------------------

  def exec([]) do
    :execute_transaction
  end

  def exec(_) do
    {:error, {:wrong_number_of_arguments, :"EXEC"}}
  end

  ## ------------------------------------------------------------------
  ## MULTI Command
  ## ------------------------------------------------------------------

  def multi([]) do
    :start_transaction
  end

  def multi(_) do
    {:error, {:wrong_number_of_arguments, :"MULTI"}}
  end
end
