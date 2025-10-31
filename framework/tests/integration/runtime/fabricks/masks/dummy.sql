create function mask_dummy(dummy string) return if(dummy == '1', '***', dummy)
