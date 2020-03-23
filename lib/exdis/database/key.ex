defmodule Exdis.Database.Key do
  ## ------------------------------------------------------------------
  ## DELETE Command
  ## ------------------------------------------------------------------

  def delete(database, key) do
    Exdis.Database.KeyOwner.manipulate_if_set(
      database, key, &handle_delete/1,
      {:error, :key_not_found})
  end

  defp handle_delete(state) do
      case state do
        nil ->
          {:error, :key_not_found}
        _ ->
          {:ok_and_update, nil}
      end
  end
end
