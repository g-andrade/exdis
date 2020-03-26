defmodule Exdis.List do
  ## ------------------------------------------------------------------
  ## take!
  ## ------------------------------------------------------------------

  def take!(list, value) do
    take_recur!(list, value, [])
  end

  defp take_recur!([head|tail], value, acc) do
    case head === value do
      false ->
        take_recur!(tail, value, [head|acc])
      true ->
        :lists.reverse(acc, tail)
      end
  end

  defp take_recur!([], value, acc) do
    :erlang.error({:value_not_in_list, value, acc})
  end
end
