<template>
  <Promised :promise="promise">
    <template v-slot:pending>
      <div class="text-center">
        <v-progress-circular indeterminate color="primary"></v-progress-circular>
      </div>
    </template>
    <template v-slot="data">
      <Stats :data="data.temperature" title="Temperature" />
      <Stats :data="data.wind" title="Wind" />
      <Stats :data="data.pressure" title="Pressure" />
    </template>
    <template v-slot:rejected="error">
      <p>Error: {{ error.message }}</p>
    </template>
  </Promised>
</template>

<script>
import Stats from './Stats'

export default {
  components: {
    Stats
  },
  props: {
    promise: {
      required: true,
      type: Promise
    }
  }
}
</script>
