defmodule Exdis.Database.Key do
  ## ------------------------------------------------------------------
  ## DEL Command
  ## ------------------------------------------------------------------

  def delete(key_owners) do
    delete_recur(key_owners, 0)
  end

  defp delete_recur([key_owner | next_key_owners], count_acc) do
    case Exdis.Database.KeyOwner.write(key_owner, &handle_delete/1) do
      {:ok, :deleted} ->
        delete_recur(next_key_owners, count_acc + 1)
      {:error, :key_not_set} ->
        delete_recur(next_key_owners, count_acc)
    end
  end

  defp delete_recur([], count_acc) do
    {:ok, {:integer, count_acc}}
  end

  defp handle_delete(state) do
    case state === nil do
      false -> {:ok, :deleted, nil}
      true  -> {:error, :key_not_set}
    end
  end

  ## ------------------------------------------------------------------
  ## EXIST Command
  ## ------------------------------------------------------------------

  def exist?(key_owners) do
    exist_recur(key_owners, 0)
  end

  defp exist_recur([key_owner | next_key_owners], count_acc) do
    case Exdis.Database.KeyOwner.read(key_owner, &handle_exists/1) do
      {:ok, true} ->
        exist_recur(next_key_owners, count_acc + 1)
      {:ok, false} ->
        exist_recur(next_key_owners, count_acc)
    end
  end

  defp exist_recur([], count_acc) do
    {:ok, {:integer, count_acc}}
  end

  defp handle_exists(state) do
    exists = state !== nil
    {:ok, exists}
  end
end
