<template>
  <div>
    <div>{{ title }}</div>
    <v-chip class="ma-2" color="primary" text-color="white">
      <span><b>Count</b> - {{ data.count }}</span>
    </v-chip>
    <v-chip class="ma-2" color="blue" text-color="white">
      <span><b>Minimum</b> - {{ data.min.toFixed(2) }} {{ unit }}</span>
    </v-chip>
    <v-chip class="ma-2" color="orange" text-color="white">
      <span><b>Average</b> - {{ data.avg.toFixed(2) }} {{ unit }}</span>
    </v-chip>
    <v-chip class="ma-2" color="red" text-color="white">
      <span><b>Maximum</b> - {{ data.max.toFixed(2) }} {{ unit }}</span>
    </v-chip>
  </div>
</template>

<script>
export default {
  props: {
    data: {
      required: true,
      type: Object
    },
    title: {
      required: true,
      type: String
    }
  },
  data() {
    return {
      unit: ''
    }
  },
  mounted() {
    switch (this.title) {
      case 'Pressure':
        this.unit = 'hPa'
        break
      case 'Wind':
        this.unit = 'km/h'
        break
      case 'Temperature':
        this.unit = '°C'
        break
    }
  }
}
</script>
