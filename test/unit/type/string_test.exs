defmodule ExdisTest.Unit.Type.String do
  use ExUnit.Case

  # Based on Redis own "tests/unit/type/string.tcl" suite

  setup do
    {ip_address, port} = Exdis.Listener.ip_address_and_port()
    #port = 6379
    {:ok, conn} = Redix.start_link(host: ip_address, port: port)
    {:ok, conn: conn, epoch: System.os_time(), setup_pid: self()}
  end

  test "SET and GET an item", ctx do
    assert :ok      = r(ctx, [SET, :x, "foobar"])
    assert "foobar" = r(ctx, [GET, :x])
  end

  test "SET and GET an empty item", ctx do
    assert :ok = r(ctx, [SET, :x, ""])
    assert ""  = r(ctx, [GET, :x])
  end

#  test "Very big payload in GET/SET", ctx do
#    payload  = String.duplicate("abcd", 1000000)
#    assert :ok     = r(ctx, [SET, :foo, payload])
#    assert payload = r(ctx, [GET, :foo])
#  end

#  XXX tag as slow?
#  test "Very big payload random access", ctx do
#    number_of_payloads = 100
#    payloads = Enum.reduce(1..number_of_payloads, %{},
#      fn index, acc ->
#        key = {:x, ".", "bigpayload_#{index}"}
#        pattern_repetitions = :rand.uniform(1000000)
#        payload = String.duplicate("pl-#{index}", pattern_repetitions)
#        assert :ok = r(ctx, [SET, key, payload])
#        Map.put(acc, key, payload)
#      end)
#
#    Enum.each(1..1000,
#      fn _ ->
#        {key, payload} = Enum.random(payloads)
#        assert ^payload = r(ctx, [GET, key])
#      end)
#  end

#  test "SET 10000 numeric keys and access them all in reverse order", ctx do
#    assert :ok = r(ctx, [FLUSHDB])
#    # TODO
#  end

  ## ------------------------------------------------------------------
  ## Helpers
  ## ------------------------------------------------------------------

  defp r(context, command) do
    conn = context[:conn]
    command = Enum.map(command, &map_command_part(&1, context))
      case Redix.command(conn, command) do
        {:ok, "OK"} ->
          :ok
        {:ok, success} ->
          success
        {:error, _} = error ->
          error
      end
  end

  defp map_command_part(part, context) when is_atom(part) do
    case Atom.to_string(part) do
      "Elixir." <> binary ->
        binary
      test_specific_constant ->
        context_epoch = context[:epoch]
        context_setup_pid = context[:setup_pid]
        "#{__MODULE__}.#{context_epoch}.#{inspect context_setup_pid}.#{test_specific_constant}"
    end
  end

  defp map_command_part(part, context) when is_tuple(part) do
    list = Tuple.to_list(part)
    list = Enum.map(list, &map_command_part(&1, context))
    :unicode.characters_to_binary(list)
  end

  defp map_command_part(part, _context) do
    part
  end
end
