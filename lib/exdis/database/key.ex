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
end
