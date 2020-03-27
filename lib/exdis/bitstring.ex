defmodule Exdis.Bitstring do
  ## ------------------------------------------------------------------
  ## bit_count
  ## ------------------------------------------------------------------

  def bit_count(bitstring) do
    bit_count_recur(bitstring, 0)
  end

  ## TODO optimize counting of uint64 and uint32 blocks

  defp bit_count_recur(<<byte, rest :: bits>>, acc) do
    acc = acc + Exdis.Byte.bit_count(byte)
    bit_count_recur(rest, acc)
  end

  defp bit_count_recur(<<bitstring :: bits>>, acc) do
    size = bit_size(bitstring)
    <<less_than_1_byte :: integer-size(size)>> = bitstring
    acc + Exdis.Byte.bit_count(less_than_1_byte)
  end
end
