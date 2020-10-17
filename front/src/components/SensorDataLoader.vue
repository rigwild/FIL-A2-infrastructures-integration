<template>
  <Promised :promise="promise">
    <template v-slot:pending>
      <div class="text-center">
        <v-progress-circular indeterminate color="primary"></v-progress-circular>
      </div>
    </template>
    <template v-slot="data">
      <highcharts :options="dataToChartOptions(data)"></highcharts>
    </template>
    <template v-slot:rejected="error">
      <p>Error: {{ error.message }}</p>
    </template>
  </Promised>
</template>

<script>
import Highcharts from 'highcharts'
import Stats from './Stats'

const defaultOptions = () => ({
  chart: {
    zoomType: 'x'
  },
  title: {
    text: 'USD to EUR exchange rate over time'
  },
  subtitle: {
    text:
      document.ontouchstart === undefined ? 'Click and drag in the plot area to zoom in' : 'Pinch the chart to zoom in'
  },
  xAxis: {
    type: 'datetime'
  },
  yAxis: {
    min: 0,
    title: {
      text: 'Exchange rate'
    }
  },
  legend: {
    enabled: false
  },
  plotOptions: {
    area: {
      fillColor: {
        linearGradient: {
          x1: 0,
          y1: 0,
          x2: 0,
          y2: 1
        },
        stops: [
          [0, Highcharts.getOptions().colors[0]],
          [
            1,
            Highcharts.color(Highcharts.getOptions().colors[0])
              .setOpacity(0)
              .get('rgba')
          ]
        ]
      },
      marker: {
        radius: 2
      },
      lineWidth: 1,
      states: {
        hover: {
          lineWidth: 1
        }
      },
      threshold: null
    }
  },

  series: [
    {
      type: 'area',
      name: 'USD to EUR',
      data: [
        [1167609600000, 0.7537],
        [1167696000000, 0.7537],
        [1167782400000, 0.7559],
        [1167868800000, 0.7631]
      ]
    }
  ]
})

export default {
  components: {
    Stats
  },
  props: {
    promise: {
      required: true,
      type: Promise
    },
    type: {
      required: true,
      type: String
    }
  },
  data() {
    return {
      unit: ''
    }
  },
  computed: {
    typeTitleCase() {
      return this.type.slice(0, 1).toUpperCase() + this.type.slice(1)
    }
  },
  mounted() {
    switch (this.type) {
      case 'pressure':
        this.unit = 'hPa'
        break
      case 'wind':
        this.unit = 'km/h'
        break
      case 'temperature':
        this.unit = '°C'
        break
    }
  },
  methods: {
    dataToChartOptions(data) {
      let options = defaultOptions()
      options.title = `${this.typeTitleCase} over time`
      options.yAxis.title.text = `${this.typeTitleCase} (${this.unit})`
      options.series[0].name = `${this.typeTitleCase} (${this.unit})`
      options.series[0].data = data.data.map(x => [Date.parse(x.d), Number.parseFloat(x.v.toFixed(2))])
      return options
    }
  }
}
</script>
